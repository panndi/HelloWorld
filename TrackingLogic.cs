using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using InmarsatC;
using Microsoft.Identity.Client;
using Microsoft.Practices.EnterpriseLibrary.Common.Configuration;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling.Configuration;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling.Data;
using Microsoft.Rest;
using TCPService;
using Navtor.Logging;
using Tracker.Auxiliary;
using Tracker.Communication.AisClient;
using Tracker.Communication.AisClient.Models;
using Tracker.Diagnostics;
using AzureRetryPolicy = Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling.RetryPolicy;

namespace Tracker
{
    public class DNIDGroupPoll
    {
        public DNIDGroupPoll(int dnid, string poll_to_addr, string reply_to_addr, string userid, string password)
        {
            DNID = dnid;
            PollToAddr = poll_to_addr;
            ReplyToAddr = reply_to_addr;
            UserId = userid;
            Password = password;
        }
        public int DNID;
        public string PollToAddr;
        public string ReplyToAddr;
        public string UserId;
        public string Password;
    }

    public class TrackingLogic
    {
        public List<Vessel> vessels;
        List<Vessel> vesselsToPoll;
        DateTime startInmarsatCTime;
        DateTime singlePollStart;
        double delayInmarsatC;
        DateTime startPositionRaw;
        double delayPositionRaw;
        ITrackerConfiguration _config;

        public TrackingLogic(ITrackerConfiguration config)
        {
            _config = config;
            vessels = new List<Vessel>();
            vesselsToPoll = new List<Vessel>();
            startInmarsatCTime = DateTime.UtcNow;
            singlePollStart = DateTime.UtcNow.AddHours(3);
            delayInmarsatC = 0;
            startPositionRaw = DateTime.UtcNow;
            delayPositionRaw = 0;

            using (var c = new SystemConfigurationSource())
            {
                RetryManager.SetDefault(RetryPolicyConfigurationSettings.GetRetryPolicySettings(c).BuildRetryManager());
            }
        }

        public static int Sleeper(string message, DateTime lastStart, int sleepCount)
        {
            if (DateTime.UtcNow.Subtract(lastStart).TotalMinutes > 60 * 24)
                sleepCount = 1;
            else
            {
                if (sleepCount < 10)
                    sleepCount++;
            }
            int sleepTimeInMinutes = (int)Math.Pow(2, sleepCount - 1);
            if (sleepCount > 5)
                LoggingService.LogFatal("Sleeping " + message + " in " + sleepTimeInMinutes.ToString() + " minutes");
            else
                LoggingService.LogError("Sleeping " + message + " in " + sleepTimeInMinutes.ToString() + " minutes");
            Thread.Sleep(60 * sleepTimeInMinutes * 1000);
            return sleepCount;
        }

        private static List<int> GetKystVerketVessels(ITrackerConfiguration config)
        {
            List<int> mmsi = new List<int>();
            using (SqlConnection con = GetConnection(config))
            {
                using (SqlCommand cmd = new SqlCommand("SELECT mmsi_no FROM TRACK.V_VESSEL WHERE allow_kystverket=1", con))
                {
                    using (SqlDataReader reader = cmd.ExecuteReaderWithRetry())
                    {
                        while (reader.Read())
                        {
                            if (!reader.IsDBNull(0))
                                mmsi.Add(reader.GetInt32(0));
                        }
                    }
                }
            }
            mmsi.Sort();
            return mmsi;
        }

        private static List<Tuple<List<int>, string, string>> GetFleetTrackerLogins(ITrackerConfiguration config)
        {
            List<Tuple<List<int>, string, string>> l = new List<Tuple<List<int>, string, string>>();

            using (SqlConnection con = GetConnection(config))
            {
                using (SqlCommand cmd = new SqlCommand("SELECT v.imo_no,f.client_guid,f.company_guid FROM TRACK.V_VESSEL v INNER JOIN TRACK.FLEET_TRACKER f ON v.company_id = f.company_id WHERE v.imo_no IS NOT NULL ORDER BY f.company_id", con))
                {
                    List<int> vessels_imo = new List<int>();
                    using (SqlDataReader reader = cmd.ExecuteReaderWithRetry())
                    {
                        while (reader.Read())
                        {
                            int idx = 0;
                            int imo = reader.GetInt32(idx++);
                            string clg = reader.GetString(idx++);
                            string cog = reader.GetString(idx++);
                            if (l.Count() == 0)
                            {
                                Tuple<List<int>, string, string> v = Tuple.Create(new List<int>(), clg, cog);
                                v.Item1.Add(imo);
                                l.Add(v);
                            }
                            else
                              if ((l[l.Count() - 1].Item3 == cog) && (l[l.Count() - 1].Item2 == clg))
                                l[l.Count() - 1].Item1.Add(imo);
                            else
                            {
                                Tuple<List<int>, string, string> v = Tuple.Create(new List<int>(), clg, cog);
                                v.Item1.Add(imo);
                                l.Add(v);
                            }
                        }
                    }
                }
            }
            return l;
        }

        // TODO: remove this code after 19-05-2021
        [Obsolete]
        public static void SpireAIS(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 6); //Sleep 6 minutes before starting
            lastStart = DateTime.UtcNow;
            List<int> spireVessels = GetKystVerketVessels(config);
            DateTime lastRefresh = DateTime.UtcNow;
            while (true)
            {
                try
                {
                    DateTime lastDataReceived = DateTime.UtcNow;
                    string spireIP = "streaming.ais.spire.com"; // config.GetValue("KystverketIP");
                    int spirePort = 56784;//System.Convert.ToInt32(config.GetValue("KystverketPort"));
                    int spirePosFrequencyInMinutes = 10;//System.Convert.ToInt32(config.GetValue("KystverketPosFrequecyInMinutes"));
                    string token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjdXN0b21lciI6eyJpZCI6IjQ2OSIsIm5hbWUiOiJOYXZ0b3IiLCJ1dWlkIjoiNDY5In0sImlzcyI6InNwaXJlLmNvbSIsImlhdCI6MTU0MjEyODU3MH0.EMvgzoYLIPcrQQhM-NXgLY8-LDeOzHF_aFr0UQ3w5Jk";
                    TcpReader r = new TcpReader(spireIP, spirePort, token);
                    Thread.Sleep(1000);
                    AISNMEAParser.Parser parser = new AISNMEAParser.Parser();
                    TrackReceiver receiver = new TrackReceiver(config, spirePosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                    while (r.IsConnected && (DateTime.UtcNow.Subtract(lastDataReceived).TotalMinutes < 5))
                    {
                        string s = r.Read();
                        string[] lines = s.Split('\n');
                        if (lastRefresh.AddMinutes(vesselListRefreshFrequencyInMinutes) < DateTime.UtcNow)
                        {
                            spireVessels = GetKystVerketVessels(config);
                            lastRefresh = DateTime.UtcNow;
                        }
                        foreach (string line in lines)
                            if (line.Length > 0)
                            {
                                lastStart = DateTime.UtcNow;
                                lastDataReceived = DateTime.UtcNow;
                                ExternalPosition pos;
                                ExternalDestination dest;
                                try
                                {
                                    if (AisReader.Process(TrackingSource.Spire, line, parser, out pos, out dest))
                                    {
                                        if (pos != null)
                                        {
                                            if (spireVessels.BinarySearch(pos.mmsi ?? 0) >= 0)
                                            {
                                                receiver.Process(pos, dest);
                                                sleepCount = 0; //Resetting sleep count if we successfully received a position
                                            }
                                        }
                                        else
                                        if (dest != null)
                                        {
                                            if (spireVessels.BinarySearch(dest.mmsi ?? 0) >= 0)
                                            {
                                                receiver.Process(pos, dest);
                                                sleepCount = 0; //Resetting sleep count if we successfully received a position
                                            }
                                        }
                                    }
                                }
                                catch (Exception)
                                {
                                    //   LoggingService.LogWarning("KysterketAIS : Cannot parse :(" + line + ")" + e);
                                }
                            }
                    }
                    if (!r.IsConnected)
                    {
                        LoggingService.LogError("Spire lost connection, sleep 1 minute");
                        Thread.Sleep(1 * 60 * 1000); //Sleep 1 minute
                    }
                    else
                    {
                        LoggingService.LogError("Spire Stopped receiving data, sleep 1 minute");
                        Thread.Sleep(1 * 60 * 1000); //Sleep 1 minutes
                    }
                    r.Close();
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("Spire : " + e);
                    sleepCount = Sleeper("Spire", lastStart, sleepCount);
                }
            }
        }


        public static void KystverketAIS(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 2); //Sleep 2 minutes before starting
            lastStart = DateTime.UtcNow;
            List<int> kystVerketVessels = GetKystVerketVessels(config);
            DateTime lastRefresh = DateTime.UtcNow;
            while (true)
            {
                try
                {
                    if (!bool.Parse(config.GetValue("KystverketEnabled")))
                    {
                        Thread.Sleep(5 * 60 * 1000); //Sleep 5 minutes
                        continue;
                    }

                    DateTime lastDataReceived = DateTime.UtcNow;
                    string kystverketIP = config.GetValue("KystverketIP");
                    int kystverketPort = System.Convert.ToInt32(config.GetValue("KystverketPort"));
                    int kystverketPosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("KystverketPosFrequecyInMinutes"));
                    TcpReader r = new TcpReader(kystverketIP, kystverketPort);
                    Thread.Sleep(1000);
                    AISNMEAParser.Parser parser = new AISNMEAParser.Parser();
                    TrackReceiver receiver = new TrackReceiver(config, kystverketPosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);


                    while (r.IsConnected && (DateTime.UtcNow.Subtract(lastDataReceived).TotalMinutes < 10))
                    {
                        string s = r.Read();
                        string[] lines = s.Split('\n');
                        if (lastRefresh.AddMinutes(vesselListRefreshFrequencyInMinutes) < DateTime.UtcNow)
                        {
                            kystVerketVessels = GetKystVerketVessels(config);
                            lastRefresh = DateTime.UtcNow;
                        }
                        foreach (string line in lines)
                            if (line.Length > 0)
                            {
                                lastStart = DateTime.UtcNow;
                                lastDataReceived = DateTime.UtcNow;
                                ExternalPosition pos;
                                ExternalDestination dest;
                                try
                                {
                                    if (AisReader.Process(TrackingSource.Kystverket, line, parser, out pos, out dest))
                                    {
                                        if (pos != null)
                                        {
                                            if (kystVerketVessels.BinarySearch(pos.mmsi ?? 0) >= 0)
                                            {
                                                receiver.Process(pos, dest);
                                                sleepCount = 0; //Resetting sleep count if we successfully received a position
                                            }
                                        }
                                        else
                                        if (dest != null)
                                        {
                                            if (kystVerketVessels.BinarySearch(dest.mmsi ?? 0) >= 0)
                                            {
                                                receiver.Process(pos, dest);
                                                sleepCount = 0; //Resetting sleep count if we successfully received a position
                                            }
                                        }
                                    }
                                }
                                catch (Exception)
                                {
                                    //   LoggingService.LogWarning("KysterketAIS : Cannot parse :(" + line + ")" + e);
                                }
                            }
                    }

                    if (!r.IsConnected)
                    {
                        LoggingService.LogInformation("KystverketAIS Lost connection, sleep 5 minutes");
                        Thread.Sleep(5 * 60 * 1000); //Sleep 5 minutes
                    }
                    else
                    {
                        LoggingService.LogInformation("KystverketAIS Stopped receiving data, sleep 5 minutes");
                        Thread.Sleep(5 * 60 * 1000); //Sleep 5 minutes
                    }
                    r.Close();
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("KysterketAIS : " + e);
                    sleepCount = Sleeper("KystverketAIS", lastStart, sleepCount);
                }
            }
        }

        public static void KystverketSATAIS(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 2); //Sleep 2 minutes before starting
            lastStart = DateTime.UtcNow;
            List<int> kystVerketVessels = GetKystVerketVessels(config);
            DateTime lastRefresh = DateTime.UtcNow;
            while (true)
            {
                try
                {
                    if (!bool.Parse(config.GetValue("KystverketEnabled")))
                    {
                        Thread.Sleep(5 * 60 * 1000); //Sleep 5 minutes
                        continue;
                    }

                    DateTime lastDataReceived = DateTime.UtcNow;
                    string kystverketIP = config.GetValue("KystverketSATAISIP");
                    int kystverketPort = System.Convert.ToInt32(config.GetValue("KystverketSATAISPort"));
                    int kystverketPosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("KystverketPosFrequecyInMinutes"));
                    TcpReader r = new TcpReader(kystverketIP, kystverketPort);
                    Thread.Sleep(1000);
                    AISNMEAParser.Parser parser = new AISNMEAParser.Parser();
                    TrackReceiver receiver = new TrackReceiver(config, kystverketPosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);

                    while (r.IsConnected && (DateTime.UtcNow.Subtract(lastDataReceived).TotalMinutes < 30))
                    {
                        string s = r.Read();
                        string[] lines = s.Split('\n');
                        if (lastRefresh.AddMinutes(vesselListRefreshFrequencyInMinutes) < DateTime.UtcNow)
                        {
                            kystVerketVessels = GetKystVerketVessels(config);
                            lastRefresh = DateTime.UtcNow;
                        }
                        foreach (string line in lines)
                            if (line.Length > 0)
                            {
                                lastStart = DateTime.UtcNow;
                                lastDataReceived = DateTime.UtcNow;
                                ExternalPosition pos;
                                ExternalDestination dest;
                                try
                                {
                                    if (AisReader.Process(TrackingSource.KystverketSATAIS, line, parser, out pos, out dest))
                                    {
                                        if (pos != null)
                                        {
                                            if (kystVerketVessels.BinarySearch(pos.mmsi ?? 0) >= 0)
                                            {
                                                receiver.Process(pos, null);
                                                sleepCount = 0; //Resetting sleep count if we successfully received a position
                                            }
                                        }
                                        else
                                        if (dest != null)
                                        {
                                            if (kystVerketVessels.BinarySearch(dest.mmsi ?? 0) >= 0)
                                            {
                                                receiver.Process(null, dest);
                                                sleepCount = 0; //Resetting sleep count if we successfully received a position
                                            }
                                        }
                                    }
                                }
                                catch (Exception)
                                {
                                    //   LoggingService.LogWarning("KysterketAIS : Cannot parse :(" + line + ")" + e);
                                }
                            }
                    }
                    if (!r.IsConnected)
                    {
                        LoggingService.LogInformation("KystverketSATAIS Lost connection, sleep 5 minutes");
                        Thread.Sleep(5 * 60 * 1000); //Sleep 5 minutes
                    }
                    else
                    {
                        LoggingService.LogInformation("KystverketSATAIS Stopped receiving data, sleep 5 minutes");
                        Thread.Sleep(5 * 60 * 1000); //Sleep 5 minutes
                    }
                    r.Close();
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("KysterketSATAIS : " + e);
                    sleepCount = Sleeper("KystverketSATAIS", lastStart, sleepCount);
                }
            }
        }

        private static UInt32 imeiToVesselId(ITrackerConfiguration config, Int64 imei)
        {
            using (SqlConnection con = GetConnection(config))
            {
                SqlCommand cmd = con.CreateCommand();
                cmd.CommandText = String.Format("SELECT vessel_id FROM TRACK.FBB_TERMINAL WHERE IMEI = @imei and active=1");
                cmd.Parameters.AddWithValue("@imei", imei);
                using (SqlDataReader reader = cmd.ExecuteReaderWithRetry())
                {
                    if (reader.Read())
                        return (UInt32)reader.GetInt32(0);
                }
            }
            return 0;
        }

        private static int imeiToIMO(ITrackerConfiguration config, Int64 imei)
        {
            using (SqlConnection con = GetConnection(config))
            {
                SqlCommand cmd = con.CreateCommand();
                cmd.CommandText = String.Format("SELECT V.imo_no FROM TRACK.FBB_TERMINAL T INNER JOIN TRACK.V_VESSEL V ON T.vessel_id=V.id WHERE T.IMEI = @imei and T.active=1 AND V.imo_no IS NOT NULL");
                cmd.Parameters.AddWithValue("@imei", imei);
                using (SqlDataReader reader = cmd.ExecuteReaderWithRetry())
                {
                    if (reader.Read())
                        return reader.GetInt32(0);
                }
            }
            return 0;
        }


        private static void GetAisDbStatus(SqlConnection connection, out long lastPosProcessed, out long lastDestProcessed)
        {
            lastPosProcessed = 0;
            lastDestProcessed = 0;
            using (SqlCommand command = new SqlCommand("TRACK.GetAisDbStatus", connection) { CommandType = CommandType.StoredProcedure })
            {
                using (SqlDataReader reader = command.ExecuteReaderWithRetry())
                {
                    while (reader.Read())
                    {
                        string mType = reader.GetString(0);
                        long id = reader.GetInt64(1);
                        if (mType == "Position")
                            lastPosProcessed = id;
                        else
                            if (mType == "Destination")
                            lastDestProcessed = id;
                        else
                            throw new Exception("Illegal message_type " + mType);
                    }
                }
            }
        }

        private static void SetAisDbStatus(SqlConnection db, long lastPosProcessed, long lastDestProcessed)
        {
            using (SqlCommand command = new SqlCommand("TRACK.UpdateAisDbStatus", db) { CommandType = CommandType.StoredProcedure })
            {
                command.Parameters.AddWithValue("message_type", "Position");
                command.Parameters.AddWithValue("last_id", lastPosProcessed);
                command.ExecuteNonQueryWithRetry();
                command.Parameters[0].Value = "Destination";
                command.Parameters[1].Value = lastDestProcessed;
                command.ExecuteNonQueryWithRetry();
            }
        }

        private static AisClient CreateAisDataApiClient(ITrackerConfiguration config)
        {
            string address = config.GetValue("AisApiBaseAddress");
            string authUrl = config.GetValue("AuthUrl");
            string clientId = config.GetValue("ClientId");
            string clientSecret = config.GetValue("ClientSecret");
            string userName = config.GetValue("TrackingUser");
            string password = config.GetValue("TrackingPassword");

            ITokenProviderEx tokenProvider = new TokenProvider(authUrl, clientId, clientSecret, userName, password);
            return new AisClient(
                new Uri(address),
                new TokenCredentials(tokenProvider),
                new RestApiResponseHandler(tokenProvider));
        }

        private static void RefreshAisDBVesselList(ITrackerConfiguration config)
        {
            IMOMMSI[] toTrack = GetVesselsToTrackingUsingAisDb(config);
            List<int?> imo = new List<int?>();
            List<int?> mmsi = new List<int?>();
            for (int i = 0; i < toTrack.Count(); i++)
            {
                if (toTrack[i].IMO > 1000000)
                    imo.Add(toTrack[i].IMO);
                mmsi.Add(toTrack[i].MMSI);
            }

            using (AisClient client = CreateAisDataApiClient(config))
            {
                client.UpdateTrackedVessels(new TrackedVesselsUpdateRequest(imo, mmsi));
            }
        }

        private static void SetAIS_MMSI_Mistmatch(ITrackerConfiguration config, List<Tuple<int, int, DateTime>> mmsiMismatch)
        {

            using (SqlConnection con = GetConnection(config))
            {
                using (SqlCommand cmd = con.CreateCommand())
                {
                    cmd.CommandText = "DELETE FROM TRACK.AISDB_MMSI";
                    cmd.ExecuteNonQueryWithRetry();
                    cmd.Parameters.Clear();
                    foreach (Tuple<int, int, DateTime> t in mmsiMismatch)
                    {
                        if (t.Item3 > DateTime.UtcNow.AddDays(-4))
                        {
                            cmd.CommandText = "INSERT INTO TRACK.AISDB_MMSI(vessel_id,mmsi) VALUES(@vessel_id,@mmsi)";
                            cmd.Parameters.AddWithValue("vessel_id", t.Item1);
                            cmd.Parameters.AddWithValue("mmsi", t.Item2);
                            cmd.ExecuteNonQueryWithRetry();
                            cmd.Parameters.Clear();
                        }
                    }
                }
            }
        }

        public static void FillPositionRawFromAISDB(ITrackerConfiguration config)
        {
            DateTime lastStart, lastRun;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 1); //Sleep 1 minutes before starting
            lastStart = DateTime.UtcNow;
            DateTime lastVesselListRefresh = DateTime.UtcNow.AddHours(-1);
            lastRun = DateTime.UtcNow.AddHours(-1);
            List<Tuple<int, int, DateTime>> mmsiMismatch = new List<Tuple<int, int, DateTime>>();
            DateTime mmsiMismatchSave = DateTime.UtcNow.AddHours(-1);
            while (true)
            {
                try
                {
                    int aisDBFrequencyInSeconds = System.Convert.ToInt32(config.GetValue("AisDBFrequencyInSeconds"));
                    int aisDBVesselListRefreshInMinutes = System.Convert.ToInt32(config.GetValue("AISDBVesselListRefreshInMinutes"));
                    int aisDBPosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("AISDBPosFrequencyInMinutes"));
                    Thread.Sleep(1000);
                    TrackReceiver receiver = new TrackReceiver(config, aisDBPosFrequencyInMinutes, aisDBVesselListRefreshInMinutes);
                    while (true)
                    {
                        if (bool.Parse(config.GetValue("AisEnabled")))
                        {
                            if (DateTime.UtcNow.Subtract(lastVesselListRefresh).TotalMinutes > aisDBVesselListRefreshInMinutes)
                            {
                                lastVesselListRefresh = DateTime.UtcNow;
                                RefreshAisDBVesselList(config);
                            }

                            if (DateTime.UtcNow.Subtract(lastRun).TotalSeconds > aisDBFrequencyInSeconds)
                            {
                                long lastPosProcessed, lastDestProcessed;
                                int maxRecords = System.Convert.ToInt32(config.GetValue("AisServiceMaxRecords"));
                                using (SqlConnection connection = GetConnection(config))
                                {
                                    GetAisDbStatus(connection, out lastPosProcessed, out lastDestProcessed);
                                }
                                lastRun = DateTime.UtcNow;
                                using (AisClient client = CreateAisDataApiClient(config))
                                {
                                    var pos = client.GetLatestPositionsForTrackedVessels(lastPosProcessed, maxRecords);
                                    var dest = client.GetLatestDestinationsForTrackedVessels(lastDestProcessed, maxRecords);

                                    if (pos == null)
                                        throw new Exception("GetLatestPositionsForTrackedVessels failed");
                                    foreach (var pp in pos)
                                    {
                                        if (!Enum.TryParse(pp.Source, out TrackingSource source))
                                        {
                                            LoggingService.LogError(
                                                $"Unexpected source {pp.Source} received from GetLatestPositionsForTrackedVessels");
                                            continue;
                                        }
                                        ExternalPosition e = new ExternalPosition
                                        {
                                            imo = pp.Imo,
                                            lat = pp.Latitude,
                                            lon = pp.Longitude,
                                            mmsi = pp.Mmsi,
                                            NavStatus = pp.NavigationalStatus,
                                            heading = pp.Course.GetValueOrDefault(),
                                            speed = pp.Speed.GetValueOrDefault(),
                                            source = source,
                                            timeStamp = pp.Timestamp,
                                        };
                                        if (pp.Mmsi != 400000000) //Fake IMO and MMSI
                                        {
                                            receiver.Process(e, null);
                                            if (pp.Id > lastPosProcessed)
                                                lastPosProcessed = pp.Id;
                                        }
                                    }
                                    if (dest == null)
                                        throw new Exception("GetLatestDestinationsForTrackedVessels failed");

                                    foreach (var dd in dest)
                                    {
                                        if (dd.Mmsi != 400000000) //Fake IMO and MMSI
                                        {
                                            ExternalDestination d = new ExternalDestination();
                                            d.imo = dd.Imo;
                                            d.dest = dd.Destination;
                                            d.eta = dd.Eta;
                                            d.mmsi = dd.Mmsi;
                                            d.timeStamp = dd.Timestamp;
                                            d.NavStatus = null;
                                            d.source = TrackingSource.NavBoxAis;
                                            try
                                            {
                                                if (dd.Imo != null)
                                                {
                                                    if (DateTime.UtcNow.AddDays(-4) < dd.Timestamp)
                                                    {
                                                        int vesselID;
                                                        if (receiver.Mismatch((int)dd.Imo, dd.Mmsi, out vesselID))
                                                        {
                                                            var result = mmsiMismatch.FirstOrDefault(w => w.Item1 == vesselID && w.Item2 == dd.Mmsi);
                                                            if (result == null)
                                                                mmsiMismatch.Add(new Tuple<int, int, DateTime>(vesselID, dd.Mmsi, dd.Timestamp));
                                                        }
                                                    }
                                                }
                                            }
                                            catch { }

                                            receiver.Process(null, d);

                                            if (dd.Id > lastDestProcessed)
                                                lastDestProcessed = dd.Id;
                                        }
                                    }
                                    using (SqlConnection connection = GetConnection(config))
                                    {
                                        SetAisDbStatus(connection, lastPosProcessed, lastDestProcessed);
                                    }
                                }
                                if (mmsiMismatchSave < DateTime.UtcNow)
                                {
                                    mmsiMismatchSave = DateTime.UtcNow.AddHours(1);
                                    SetAIS_MMSI_Mistmatch(config, mmsiMismatch);
                                }

                                sleepCount = 0;
                            }
                        }

                        Thread.Sleep(5000);
                    }
                }
                catch (Exception e)
                {
                    if (sleepCount > 4)
                        LoggingService.LogFatal("FillPositionRawFromAISDB" + e);
                    else if (sleepCount > 1)
                        LoggingService.LogError("FillPositionRawFromAISDB " + e);
                    else
                        LoggingService.LogWarning("FillPositionRawFromAISDB " + e);
                    sleepCount = Sleeper("FillPositionRawFromAISDB", lastStart, sleepCount);
                }
            }
        }

        public static void FleetTracker(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart, lastRun;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 1); //Sleep 1 minutes before starting
            lastStart = DateTime.UtcNow;
            lastRun = DateTime.UtcNow.AddHours(-1);
            while (true)
            {
                try
                {
                    int fleetTrackerPosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("FleetTrackerFrequencyInMinutes"));
                    Thread.Sleep(1000);
                    TrackReceiver receiver = new TrackReceiver(config, fleetTrackerPosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                    while (true)
                    {
                        if (bool.Parse(config.GetValue("FleetTrackerEnabled")) && DateTime.UtcNow.Subtract(lastRun).TotalMinutes > fleetTrackerPosFrequencyInMinutes)
                        {
                            List<Tuple<List<int>, string, string>> logins;
                            logins = GetFleetTrackerLogins(config);
                            foreach (Tuple<List<int>, string, string> l in logins)
                            {
                                List<ExternalPosition> positions;
                                List<ExternalDestination> destinations;
                                if (l.Item1.Count > 0)
                                {
                                    FleetTrackerReader.Read(l.Item3, l.Item2, l.Item1.ToArray(), out positions, out destinations);
                                    foreach (ExternalPosition e in positions)
                                        receiver.Process(e, null);
                                    foreach (ExternalDestination d in destinations)
                                        receiver.Process(null, d);
                                }
                            }
                            lastRun = DateTime.UtcNow;
                            sleepCount = 0;
                        }

                        Thread.Sleep(5000);
                    }
                }
                catch (Exception e)
                {
                    if (sleepCount > 3)
                        LoggingService.LogFatal("FleetTracker : " + e);
                    else
                        LoggingService.LogWarning("FleetTracker : " + e);
                    sleepCount = Sleeper("FleetTracker", lastStart, sleepCount);
                }
            }
        }

        public static void AddValue(ITrackerConfiguration config)
        {
            while (true)
            {
                try
                {
                    DateTime lastStart;
                    int sleepCount = 0;
                    Thread.Sleep(60 * 1000 * 1); //Sleep 1 minutes before starting
                    lastStart = DateTime.UtcNow;
                    IPAddress localAddr = IPAddress.Parse("0.0.0.0");
                    TcpListener m_server = new TcpListener(localAddr, 7477);
                    m_server.Start();
                    while (true)
                    {
                        try
                        {
                            Socket clientSocket = m_server.AcceptSocket();
                            var socketListener = new TCPSocketThread(clientSocket);
                            socketListener.StartSocketListener(config);
                            sleepCount = 0;
                        }
                        catch (Exception e)
                        {
                            LoggingService.LogFatal("AddValue : " + e);
                            sleepCount = Sleeper("AddValue", lastStart, sleepCount);
                        }
                    }
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("AddValue Main Loop Exception : " + e);
                    Thread.Sleep(1000 * 60 * 10);
                }
            }
        }

        public static void CobhamFBB(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 2); //Sleep 2 minutes before starting
            lastStart = DateTime.UtcNow;
            while (true)
            {
                if (!bool.Parse(config.GetValue("CobhamFbbEnabled")))
                {
                    Thread.Sleep(TimeSpan.FromMinutes(1));
                    continue;
                };

                try
                {
                    DateTime lastDataReceived = DateTime.UtcNow;
                    int cobhamFBBPortReceive = System.Convert.ToInt32(config.GetValue("CobhamFBBPortReceive"));
                    int cobhamPosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("CobhamPosFrequencyInMinutes"));
                    UdpClient udpClient = new UdpClient(cobhamFBBPortReceive);
                    FBBReader fbb = new FBBReader();
                    Int64 imei;
                    UInt32 vesselId;
                    ExternalPosition p;
                    Thread.Sleep(1000);
                    TrackReceiver receiver = new TrackReceiver(config, cobhamPosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                    while (true)
                    {
                        var remoteEP = new IPEndPoint(IPAddress.Any, 0);
                        var data = udpClient.Receive(ref remoteEP);
                        if (fbb.Process(data, out imei, out vesselId, out p))
                        {
                            if (vesselId == 0)
                            {
                                vesselId = imeiToVesselId(config, imei);
                                if (vesselId != 0)
                                {
                                    byte[] ack = fbb.GetAck(vesselId);
                                    udpClient.Send(ack, ack.Length, remoteEP);
                                    //Todo log this in Database
                                    //LoggingService.LogError("VesselID:" + vesselId.ToString() + " Sending FBB register information to port " + remoteEP.Port.ToString());
                                }
                                else
                                    LoggingService.LogFatal("Cannot find FBB terminal with imei " + imei.ToString());
                            }
                            else
                            {
                                if (p != null)
                                {
                                    if (!receiver.LogPosition(p, (int)vesselId))
                                        LoggingService.LogError("VesselID:" + vesselId.ToString() + " FBB Cannot log position" + imei.ToString());
                                }
                                else
                                    LoggingService.LogError("VesselID:" + vesselId.ToString() + " FBB p==null" + imei.ToString());
                            }
                        }
                        else
                            LoggingService.LogInformation("Cannot process FBB data " + ByteArrayToString(data) + " vesselId=" + vesselId.ToString() + " IP=" + remoteEP.ToString());
                    }
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("CobhamFBB : " + e);
                    sleepCount = Sleeper("CobhamFBB", lastStart, sleepCount);
                }
            }
        }


        public static void ExactEarth(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart, lastRun;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 1); //Sleep 1 minutes before starting
            lastStart = DateTime.UtcNow;
            lastRun = DateTime.UtcNow.AddMinutes(-10);
            while (true)
            {
                try
                {
                    int readFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("ExactEarthReadFrequencyInMinutes"));
                    Thread.Sleep(1000);
                    TrackReceiver receiver = new TrackReceiver(config, readFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                    while (true)
                    {
                        string url = "https://gws.exactearth.com/ows?service=wfs&version=1.1.0&request=GetFeature&typeName=exactAIS:LVI&filter=" +
"(<Filter><PropertyIsGreaterThanOrEqualTo><PropertyName>ts_insert_utc</PropertyName><Literal>" + lastRun.AddMinutes(-2).ToString("yyyyMMddHHmmss") + "</Literal></PropertyIsGreaterThanOrEqualTo></Filter>)";
                        url = url + "&authKey=" + config.GetValue("ExactEarthToken");

                        if (DateTime.UtcNow.Subtract(lastRun).TotalMinutes > readFrequencyInMinutes)
                        {
                            ExternalPosition[] positions;
                            ExternalDestination[] destinations;
                            string errorMessage = "";
                            lastRun = DateTime.UtcNow;
                            positions = ExactEarthReader.Read(url, out destinations, out errorMessage);
                            if (positions == null)
                                throw new Exception(errorMessage);
                            foreach (ExternalPosition e in positions)
                                receiver.Process(e, null);
                            if (destinations != null)
                            {
                                foreach (ExternalDestination d in destinations)
                                    receiver.Process(null, d);
                            }
                            sleepCount = 0;
                        }
                        Thread.Sleep(5000);
                    }
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("ExactEarth : " + e);
                    sleepCount = Sleeper("ExactEarth", lastStart, sleepCount);
                }
            }

        }


        private static string ByteArrayToString(byte[] ba)
        {
            if ((ba == null) || (ba.Length == 0))
                return ("Empty");
            string hex = BitConverter.ToString(ba);
            return hex.Replace("-", "");
        }

        public static void GSatTracker(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 2); //Sleep 2 minutes before starting
            lastStart = DateTime.UtcNow;
            while (true)
            {
                if (!bool.Parse(config.GetValue("GSatTrackerEnabled")))
                {
                    Thread.Sleep(TimeSpan.FromMinutes(1));
                    continue;
                };

                try
                {
                    DateTime lastDataReceived = DateTime.UtcNow;
                    int gSatTrackerPortReceive = System.Convert.ToInt32(config.GetValue("GSatTrackerPortReceive"));
                    int gSatTrackerPosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("CobhamPosFrequencyInMinutes"));
                    UdpClient udpClient = new UdpClient(gSatTrackerPortReceive);
                    Thread.Sleep(1000);
                    TrackReceiver receiver = new TrackReceiver(config, gSatTrackerPosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                    while (true)
                    {
                        var remoteEP = new IPEndPoint(IPAddress.Any, 0);
                        var data = udpClient.Receive(ref remoteEP);
                        if (data.Length > 0)
                            LoggingService.LogError("GSatTracker" + ByteArrayToString(data));
                        else
                            LoggingService.LogFatal("No data from GSatTracker");
                    }
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("GSatTracker : " + e);
                    sleepCount = Sleeper("GSatTracker", lastStart, sleepCount);
                }
            }
        }

        public static void SetelTracker(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 2); //Sleep 2 minutes before starting
            lastStart = DateTime.UtcNow;
            while (true)
            {
                if (!bool.Parse(config.GetValue("SetelEnabled")))
                {
                    Thread.Sleep(TimeSpan.FromMinutes(1));
                    continue;
                };

                try
                {
                    DateTime lastDataReceived = DateTime.UtcNow;
                    int setelTrackerPortReceive = System.Convert.ToInt32(config.GetValue("SetelTrackerPortReceive"));
                    int setelTrackerPosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("SetelTrackerPosFrequencyInMinutes"));
                    UdpClient udpClient = new UdpClient(setelTrackerPortReceive);
                    ExternalPosition p;
                    Thread.Sleep(1000);
                    TrackReceiver receiver = new TrackReceiver(config, setelTrackerPosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                    while (true)
                    {
                        var remoteEP = new IPEndPoint(IPAddress.Any, 0);
                        var data = udpClient.Receive(ref remoteEP);
                        if (data.Length > 0)
                        {
                            string errorString;
                            p = SetelReader.Read(data, out errorString);
                            if (errorString != "")
                                LoggingService.LogError("setelTracker" + ByteArrayToString(data));
                            else
                                receiver.Process(p, null);
                        }
                        else
                            LoggingService.LogFatal("No data from SetelTracker");
                    }
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("SetelTracker : " + e);
                    sleepCount = Sleeper("SetelTracker", lastStart, sleepCount);
                }
            }
        }

        private static bool NeedVesselPos(TrackReceiver r, int mmsi, SqlConnection db)
        {
            int vessel_id;
            if (r.MMSIToVesselID(mmsi, out vessel_id))
            {
                using (SqlCommand cmd = new SqlCommand("TRACK.NeedPosition", db) { CommandType = CommandType.StoredProcedure })
                {
                    cmd.Parameters.AddWithValue("vessel_id", vessel_id);
                    SqlParameter returnValue = new SqlParameter("returnVal", SqlDbType.Int) { Direction = ParameterDirection.ReturnValue };
                    cmd.Parameters.Add(returnValue);
                    cmd.ExecuteNonQueryWithRetry();
                    return Convert.ToInt32(returnValue.Value) == 1;
                }

            }
            return false;
        }


        private static IMOMMSI[] GetVesselsToTrackMarineTraffic(ITrackerConfiguration config)
        {
            List<IMOMMSI> vessels = new List<IMOMMSI>();
            using (SqlConnection con = GetConnection(config))
            {
                using (SqlCommand cmd = new SqlCommand("TRACK.GetVesselsToTrackUsingSource", con) { CommandType = CommandType.StoredProcedure })
                {
                    cmd.Parameters.AddWithValue("source_id", (int)TrackingSource.MarineTraffic);
                    using (SqlDataReader reader = cmd.ExecuteReaderWithRetry())
                    {
                        while (reader.Read())
                        {
                            int id, imo, mmsi;
                            id = (int)reader["id"];
                            mmsi = reader.IsDBNull(reader.GetOrdinal("mmsi_no")) ? 0 : (int)reader["mmsi_no"];
                            imo = reader.IsDBNull(reader.GetOrdinal("imo_no")) ? 0 : (int)reader["imo_no"];
                            IMOMMSI m = new IMOMMSI();
                            m.VesselId = id;
                            m.IMO = imo;
                            m.MMSI = mmsi;
                            vessels.Add(m);
                        }
                    }
                }
            }
            return vessels.ToArray();
        }

        private static IMOMMSI[] GetVesselsToTrackingUsingAisDb(ITrackerConfiguration config)
        {
            List<IMOMMSI> vessels = new List<IMOMMSI>();
            using (SqlConnection con = GetConnection(config))
            {
                using (SqlCommand cmd = new SqlCommand("TRACK.GetVesselsToTrackUsingSource", con) { CommandType = CommandType.StoredProcedure })
                {
                    cmd.Parameters.AddWithValue("source_id", (int)TrackingSource.NavBoxAisOther);
                    using (SqlDataReader reader = cmd.ExecuteReaderWithRetry())
                    {
                        while (reader.Read())
                        {
                            int id, imo, mmsi;
                            id = (int)reader["id"];
                            mmsi = reader.IsDBNull(reader.GetOrdinal("mmsi_no")) ? 0 : (int)reader["mmsi_no"];
                            imo = reader.IsDBNull(reader.GetOrdinal("imo_no")) ? 0 : (int)reader["imo_no"];
                            IMOMMSI m = new IMOMMSI();
                            m.VesselId = id;
                            m.IMO = imo;
                            m.MMSI = mmsi;
                            vessels.Add(m);
                        }
                    }
                }
            }
            return vessels.ToArray();
        }

        private static int[] GetIMOOfVesselsToTrackUsingMariTrace(ITrackerConfiguration config)
        {
            List<int> vessels = new List<int>();
            using (SqlConnection con = GetConnection(config))
            {
                using (SqlCommand cmd = new SqlCommand("TRACK.GetVesselsToTrackUsingSource", con) { CommandType = CommandType.StoredProcedure })
                {
                    cmd.Parameters.AddWithValue("source_id", (int)TrackingSource.MariTrace);
                    using (SqlDataReader reader = cmd.ExecuteReaderWithRetry())
                    {
                        while (reader.Read())
                        {
                            int imo;
                            imo = reader.IsDBNull(reader.GetOrdinal("imo_no")) ? 0 : (int)reader["imo_no"];
                            if (imo > 0)
                                vessels.Add(imo);
                        }
                    }
                }
            }
            return vessels.ToArray();
        }

        private static void RefreshMarineTrafficVesselList(ITrackerConfiguration config)
        {
            /*            try
                        { */
            string password50 = config.GetValue("MarineTrafficService50");
            string password51 = config.GetValue("MarineTrafficService51");
            IMOMMSI[] toTrack = GetVesselsToTrackMarineTraffic(config);
            if (toTrack.Count() < 1000)
                throw new Exception("MarineTraffic Too few vessels to track");
            Tuple<int, int>[] mmsiMismatch;
            MarineTrafficReader.SetVesselsToTrack("http://services.marinetraffic.com/api/getfleet/" + password51 + "/protocol:csv", toTrack, password50, out mmsiMismatch);
            using (SqlConnection con = GetConnection(config))
            {
                using (SqlCommand cmd = con.CreateCommand())
                {
                    cmd.CommandText = "DELETE FROM TRACK.MARINE_TRAFFIC_MMSI";
                    cmd.ExecuteNonQueryWithRetry();
                    cmd.Parameters.Clear();
                    foreach (Tuple<int, int> t in mmsiMismatch)
                    {
                        cmd.CommandText = "INSERT INTO TRACK.MARINE_TRAFFIC_MMSI(vessel_id,mmsi) VALUES(@vessel_id,@mmsi)";
                        cmd.Parameters.AddWithValue("vessel_id", t.Item1);
                        cmd.Parameters.AddWithValue("mmsi", t.Item2);
                        cmd.ExecuteNonQueryWithRetry();
                        cmd.Parameters.Clear();
                    }
                }
            }
            /*            }
                        catch (Exception e)
                        {
                            LoggingService.LogError("RefreshTracking MarineTrafficXml : " + e);
                        }
                        */
        }

        public static void MariTrace(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart;
            int sleepCount = 0;
            DateTime lastPosSimple = DateTime.UtcNow.AddHours(-1);
            DateTime lastVesselListRefresh = DateTime.UtcNow.AddHours(-1);
            int mariTraceVesselListRefreshInMinutes = System.Convert.ToInt32(config.GetValue("MariTraceVesselListRefreshInMinutes"));
            string password = config.GetValue("MariTracePassword");
            string username = config.GetValue("MariTraceUser");
            string baseurl = config.GetValue("MariTraceUrl");
            while (true)
            {
                lastStart = DateTime.UtcNow;
                try
                {
                    int mariTracePosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("MariTracePosFrequencyInMinutes"));
                    Thread.Sleep(1000);
                    TrackReceiver receiver = new TrackReceiver(config, mariTracePosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                    MariTraceReader mariTrace = new MariTraceReader(baseurl, username, password, new DiagnosticsLogger( config ));
                    while (true)
                    {
                        if (bool.Parse(config.GetValue("MariTraceEnabled")))
                        {
                            if (DateTime.UtcNow.Subtract(lastVesselListRefresh).TotalMinutes > mariTraceVesselListRefreshInMinutes)
                            {
                                lastVesselListRefresh = DateTime.UtcNow;
                                int[] vesselsToTrack = GetIMOOfVesselsToTrackUsingMariTrace(config);
                                mariTrace.SetFleetToTrack(vesselsToTrack);
                            }
                            if (DateTime.UtcNow.Subtract(lastPosSimple).TotalMinutes > mariTracePosFrequencyInMinutes)
                            {
                                ExternalPosition[] pos;
                                ExternalDestination[] dest;
                                lastPosSimple = DateTime.UtcNow;
                                mariTrace.GetVesselData(out pos, out dest);
                                foreach (ExternalPosition p in pos)
                                    receiver.Process(p, null);
                                foreach (ExternalDestination d in dest)
                                    receiver.Process(null, d);
                            }
                        }
                        sleepCount = 0;
                        Thread.Sleep(5000 * 60); //Sleep one minute
                    }
                }
                catch (Exception e)
                {
                    if (sleepCount > 4)
                        LoggingService.LogFatal("MariTrace : " + e);
                    else
                        LoggingService.LogError("MariTrace : " + e);
                    sleepCount = Sleeper("MariTrace", lastStart, sleepCount);
                }
            }
        }

        /*
                public static void MariTrace2(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
                {
                    DateTime lastStart;
                    int sleepCount = 0;
                    DateTime lastPosSimple = DateTime.UtcNow.AddHours(-1);
                    DateTime lastVesselListRefresh = DateTime.UtcNow.AddHours(-1);
                    int mariTraceVesselListRefreshInMinutes = System.Convert.ToInt32(config.GetValue("MariTraceVesselListRefreshInMinutes"));
                    string password = "E53672D1-DB08-43D9-BE07-16AF6E291266";
                    string username = "92058961-C507-4CF1-9570-ED98BC88982A";
                    string baseurl = config.GetValue("MariTraceUrl");
                    while (true)
                    {
                        lastStart = DateTime.UtcNow;
                        try
                        {
                            int mariTracePosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("MariTracePosFrequencyInMinutes"));
                            Thread.Sleep(1000);
                            TrackReceiver receiver = new TrackReceiver(false, config, mariTracePosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                            MariTraceReader2 mariTrace = new MariTraceReader2(baseurl, username, password);
                            while (true)
                            {
                                if (DateTime.UtcNow.Subtract(lastVesselListRefresh).TotalMinutes > mariTraceVesselListRefreshInMinutes)
                                {
                                    lastVesselListRefresh = DateTime.UtcNow;
                                    int[] vesselsToTrack = GetIMOOfVesselsToTrack(config);
                                    mariTrace.SetFleetToTrack(vesselsToTrack);
                                }
                                if (DateTime.UtcNow.Subtract(lastPosSimple).TotalMinutes > mariTracePosFrequencyInMinutes)
                                {
                                    ExternalPosition[] pos;
                                    ExternalDestination[] dest;
                                    lastPosSimple = DateTime.UtcNow;
                                    mariTrace.GetVesselData(out pos, out dest);
                                    foreach (ExternalPosition p in pos)
                                        receiver.Process(p, null);
                                    foreach (ExternalDestination d in dest)
                                        receiver.Process(null, d);
                                }
                                sleepCount = 0;
                                Thread.Sleep(5000 * 60); //Sleep one minute
                            }
                        }
                        catch (Exception e)
                        {
                            if (sleepCount > 4)
                                LoggingService.LogFatal("MariTrace : " + e);
                            else
                                LoggingService.LogError("MariTrace : " + e);
                            sleepCount = Sleeper("MariTrace2", lastStart, sleepCount);
                        }
                    }
                }
        */
        public static void MarineTrafficXml(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart;
            int sleepCount = 0;
            DateTime lastPosSimple = DateTime.UtcNow.AddHours(-1);
            DateTime lastDest = DateTime.UtcNow.AddHours(-1);
            DateTime lastVesselListRefresh = DateTime.UtcNow.AddHours(-1);
            int marineTrafficVesselListRefreshInMinutes = System.Convert.ToInt32(config.GetValue("MarineTrafficVesselListRefreshInMinutes"));
            string mtPassword = config.GetValue("MarineTrafficPassword");
            string SATPassword = config.GetValue("MarineTrafficOrbcomm");
            while (true)
            {
                lastStart = DateTime.UtcNow;
                try
                {
                    int marineTrafficPosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("MarineTrafficPosFrequencyInMinutes"));
                    int marineTrafficSleepAfterMain = System.Convert.ToInt32(config.GetValue("MarineTrafficSleepAfterMain"));
                    Thread.Sleep(1000);
                    TrackReceiver receiver = new TrackReceiver(config, marineTrafficPosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                    while (true)
                    {

                        if (bool.Parse(config.GetValue("MarineTrafficEnabled")))
                        {

                            if (DateTime.UtcNow.Subtract(lastVesselListRefresh).TotalMinutes > marineTrafficVesselListRefreshInMinutes)
                            {
                                lastVesselListRefresh = DateTime.UtcNow;
                                RefreshMarineTrafficVesselList(config);
                            }

                            if (DateTime.UtcNow.Subtract(lastPosSimple).TotalMinutes > marineTrafficPosFrequencyInMinutes)
                            {
                                if (DateTime.UtcNow.Subtract(lastDest).TotalMinutes > 60)
                                {
                                    List<ExternalDestination> dest;
                                    string destUrl;
                                    List<Tuple<int, string>> flags;
                                    for (int i = 0; i < 1; i++) //SAT is now disabled
                                    {
                                        if (i == 0)
                                            destUrl = "https://services.marinetraffic.com/api/exportvessels/" + mtPassword + "/timespan:60/msgtype:full/v:8/protocol:csv";
                                        else
                                            destUrl = "https://services.marinetraffic.com/api/exportvessels/" + SATPassword + "/timespan:720/msgtype:full/v:7/protocol:csv";
                                        MarineTrafficReader.ReadDestination(destUrl, out dest, out flags);
                                        foreach (ExternalDestination d in dest)
                                        {
                                            if (i == 1)
                                                d.source = TrackingSource.Orbcomm;
                                            receiver.Process(null, d);
                                        }
                                    }
                                    lastDest = DateTime.UtcNow;
                                    lastPosSimple = DateTime.UtcNow;
                                }
                                else
                                {
                                    List<ExternalPosition> pos;
                                    string posUrl;
                                    for (int i = 0; i < 1; i++) //SAT is now disabled
                                    {
                                        if (i == 0)
                                            posUrl = "https://services.marinetraffic.com/api/exportvessels/" + mtPassword + "/timespan:10/v:8/protocol:csv";
                                        else
                                            posUrl = "https://services.marinetraffic.com/api/exportvessels/" + SATPassword + "/timespan:720/v:7/protocol:csv";
                                        MarineTrafficReader.ReadPosition(posUrl, out pos);
                                        foreach (ExternalPosition p in pos)
                                        {
                                            if (i == 1)
                                                p.source = TrackingSource.Orbcomm;
                                            receiver.Process(p, null);
                                        }
                                    }
                                    lastPosSimple = DateTime.UtcNow;
                                }
                            }
                        }
                        sleepCount = 0;
                        Thread.Sleep(1000 * 60 * marineTrafficSleepAfterMain);
                    }
                }
                catch (Exception e)
                {
                    if (sleepCount > 4)
                        LoggingService.LogFatal("MarineTrafficXml : " + e);
                    else
                        LoggingService.LogError("MarineTrafficXml : " + e);
                    sleepCount = Sleeper("MarineTrafficXml", lastStart, sleepCount);
                }
            }
        }

        private static bool GetImoFromNavarinoRefcode(string refcode, out int imo, ITrackerConfiguration config)
        {
            if (refcode.Contains("_"))
            {
                string[] s = refcode.Split('_');
                refcode = s[0];
            }
            imo = 0;
            using (SqlConnection con = GetConnection(config))
            {
                using (SqlCommand cmd = new SqlCommand("SELECT imo_no FROM TRACK.V_VESSEL WHERE navarino_refcode = @refcode", con))
                {
                    cmd.Parameters.AddWithValue("refcode", refcode);
                    using (SqlDataReader reader = cmd.ExecuteReaderWithRetry())
                    {
                        if (reader.Read())
                        {
                            imo = reader.IsDBNull(reader.GetOrdinal("imo_no")) ? 0 : (int)reader["imo_no"];
                            if (imo > 0)
                                return true;
                        }
                    }
                }
            }
            return false;
        }


        public static void TrackingMail(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 3); //Sleep 3 minutes before starting
            while (true)
            {
                lastStart = DateTime.UtcNow;
                try
                {
                    string pop3Server = config.GetValue("ExternalTrackerPop3Server");
                    string pop3User = config.GetValue("ExternalTrackerPop3User");
                    int pop3Port = System.Convert.ToInt32(config.GetValue("ExternalTrackerPop3Port"));
                    int trackingMailSleepAfterMain = System.Convert.ToInt32(config.GetValue("TrackingMailSleepAfterMain"));
                    int trackingMailPosFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("TrackingMailPosFrequencyInMinutes"));
                    Thread.Sleep(1000);
                    TrackReceiver receiver = new TrackReceiver(config, trackingMailPosFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                    while (true)
                    {
                        if (bool.Parse(config.GetValue("EmailEnabled")))
                        {
                            string token = GetToken(config);
                            List<ExternalPosition> pos = ExternalTracker.ReadMail(config, pop3Server, pop3Port, pop3User, token, config.GetValue("ChilkatLicense"));
                            foreach (ExternalPosition p in pos)
                            {
                                bool found = false;
                                int vesselID;
                                switch (p.source)
                                {
                                    case TrackingSource.PurpleFinder:
                                    case TrackingSource.ETrack:
                                    case TrackingSource.CLS:
                                    case TrackingSource.Carras:
                                    case TrackingSource.AmosConnect:
                                    case TrackingSource.iQMarine:
                                    case TrackingSource.StratumFive:
                                    case TrackingSource.CapitalShipMgt:
                                    case TrackingSource.SpaceHorizon:
                                    case TrackingSource.Kyklades:
                                    case TrackingSource.ORCA:
                                    case TrackingSource.Rock7:
                                        if (p.imo != null && receiver.IMOToVesselID((int)p.imo, out vesselID))
                                        {
                                            p.vesselID = vesselID; // store vessel-ID, to save look-up when processing position later
                                            found = true;
                                        }
                                        break;
                                    case TrackingSource.GlobeWireless:
                                    case TrackingSource.GtShips:
                                        if (receiver.NameToVesselID(p.name, out vesselID, p.source))
                                        {
                                            p.vesselID = vesselID;
                                            found = true;
                                        }
                                        break;
                                    case TrackingSource.AtlasMaritime:
                                        if (!string.IsNullOrEmpty(p.NavarinoRefcode))
                                        {
                                            int imo;
                                            if (GetImoFromNavarinoRefcode(p.NavarinoRefcode, out imo, config))
                                            {
                                                p.imo = imo;
                                                found = true;
                                            }
                                            else
                                                LoggingService.LogFatal("TrackingMail vessel " + p.name + " with Navarino refcode=(" + p.NavarinoRefcode + ") not found");
                                        }
                                        else if (receiver.NameToVesselID(p.name, out vesselID, p.source))
                                        {
                                            p.vesselID = vesselID;
                                            found = true;
                                        }
                                        break;
                                    case TrackingSource.SkyFile:
                                        int imo_no = imeiToIMO(config, p.imei);
                                        if (imo_no > 0)
                                        {
                                            if (receiver.IMOToVesselID(imo_no, out vesselID))
                                            {
                                                p.vesselID = vesselID; // store vessel-ID, to save look-up when processing position later
                                                found = true;
                                            }
                                        }
                                        if (!found)
                                            LoggingService.LogError("TrackingMail Vessel SkyFile imei  " + p.imei.ToString() + " ...not registered for tracking");
                                        break;
                                }

                                if (!found)
                                {
                                    if (p.source == TrackingSource.AmosConnect)
                                        LoggingService.LogError("TrackingMail Vessel " + p.name + " imo " + p.imo.ToString() + " ...not registered for tracking");
                                }
                                else
                                if (found)
                                    receiver.Process(p, null);
                            }
                            sleepCount = 0;
                        }
                        Thread.Sleep(1000 * 10 * trackingMailSleepAfterMain);
                    }
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("TrackingMail : " + e);
                    sleepCount = Sleeper("TrackingMail", lastStart, sleepCount);
                }
            }
        }

        private static void ProcessInmarsatCMails(string mailAddr, ITrackerConfiguration config)
        {
            using (InmarsatCTracker inmarsatCTracker = new InmarsatCTracker(mailAddr, config, GetToken(config)))
            {
                inmarsatCTracker.inmarsatCReceiver.ProcessMails(config);
            }
        }

        public static void ProcessInmarsatC(ITrackerConfiguration config)
        {
            List<string> inmarsatCmails = new List<string>();
            inmarsatCmails = GetInmarsatCMailToAddrs(config);
            foreach (string mailAddr in inmarsatCmails)
                ProcessInmarsatCMails(mailAddr, config);
        }

        public static void InmarsatCReader(ITrackerConfiguration config)
        {
            DateTime lastStart;
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 5); //Sleep 4 minutes before starting
            while (true)
            {
                lastStart = DateTime.UtcNow;
                try
                {
                    Thread.Sleep(1000);
                    while (true)
                    {
                        if (bool.Parse(config.GetValue("InmarsatEnabled")))
                        {
                            ProcessInmarsatC(config);
                        }
                        
                        Thread.Sleep(1000 * 60); //Sleep one minute
                        sleepCount = 0;
                    }

                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("InmarsatCReader : " + e);
                    sleepCount = Sleeper("InmarsatCReader", lastStart, sleepCount);
                }
            }
        }

        public static void ReadNoonReports(ITrackerConfiguration config)
        {
            int sleepCount = 0;
            int mailProcessingFrequencyInMinutes = Convert.ToInt32(config.GetValue("NoonReportProcessingFrequencyInMinutes"));
            while (true)
            {
                DateTime lastStart = DateTime.UtcNow;
                try
                {
                    Thread.Sleep(mailProcessingFrequencyInMinutes * 60 * 1000);
                    NoonReportProcessor.ReadMail(config);
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("ReadNoonReports : " + e);
                    sleepCount = Sleeper("ReadNoonReports", lastStart, sleepCount);
                }
            }
        }

        public static void TrackingExternal(ITrackerConfiguration config, int vesselListRefreshFrequencyInMinutes)
        {
            DateTime lastStart;
            DateTime lastMaranGas = DateTime.UtcNow.AddMinutes(-10);
            DateTime lastEnamor = DateTime.UtcNow.AddMinutes(-60);
            int sleepCount = 0;
            Thread.Sleep(60 * 1000 * 4); //Sleep 4 minutes before starting
            while (true)
            {
                lastStart = DateTime.UtcNow;
                try
                {
                    int ExternalTrackerReadFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("ExternalTrackerReadFrequencyInMinutes"));
                    int MaranGasReadFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("MaranGasReadFrequencyInMinutes"));
                    int EnamorReadFrequencyInMinutes = System.Convert.ToInt32(config.GetValue("EnamorReadFrequencyInMinutes"));
                    Thread.Sleep(1000);
                    TrackReceiver receiver = new TrackReceiver(config, ExternalTrackerReadFrequencyInMinutes, vesselListRefreshFrequencyInMinutes);
                    while (true)
                    {
                        List<ExternalPosition> pos;
                        List<ExternalDestination> dest;
                        bool readMaranGas = false;
                        if (DateTime.UtcNow.Subtract(lastMaranGas).TotalMinutes > MaranGasReadFrequencyInMinutes)
                        {
                            readMaranGas = true;
                            lastMaranGas = DateTime.UtcNow;
                        }
                        bool readEnamor = false;
                        if (DateTime.UtcNow.Subtract(lastEnamor).TotalMinutes > EnamorReadFrequencyInMinutes)
                        {
                            readEnamor = true;
                            lastEnamor = DateTime.UtcNow;
                        }

                        ExternalTracker.ReadOther(config, receiver, out pos, out dest, readMaranGas, readEnamor);
                        foreach (ExternalPosition p in pos)
                            receiver.Process(p, null);
                        foreach (ExternalDestination d in dest)
                            receiver.Process(null, d);

                        sleepCount = 0;
                        Thread.Sleep(1000 * 60 * ExternalTrackerReadFrequencyInMinutes);
                    }
                }
                catch (Exception e)
                {
                    LoggingService.LogFatal("TrackingExternal : " + e);
                    sleepCount = Sleeper("TrackingExternal", lastStart, sleepCount);
                }
            }
        }
        public void SynchronizeTrackedVessels()
        {
            try
            {
                var dt = new DataTable("TRACK.VesselsTable");
                using (var erp = GetConnectionErp(_config))
                {
                    using (var cmd = new SqlCommand("ERP.GetVesselsForTracking", erp) { CommandType = CommandType.StoredProcedure })
                    {
                        using (var r = cmd.ExecuteReaderWithRetry())
                        {
                            dt.Load(r);
                        }
                    }
                }
                using (var track = GetConnection(_config))
                {
                    using (var cmd = new SqlCommand("TRACK.UpdateVessels", track) { CommandType = CommandType.StoredProcedure })
                    {
                        cmd.Parameters.Add(new SqlParameter("vessels", SqlDbType.Structured) { Value = dt });
                        cmd.ExecuteNonQueryWithRetry();
                    }
                }
            }
            catch (Exception e)
            {
                LoggingService.LogError("SynchronizeTrackedVessels: " + e);
                throw new ApplicationException("Error synchronizing tracked vessels in Track-database: " + e.Message, e);
            }
        }

        public bool RefreshVesselList()
        {
            try
            {
#if !DEBUG
                SynchronizeTrackedVessels(); // update TRACK.VESSEL from ERP
#endif
                var activevessels = new List<Vessel>();
                try
                {
                    using (var db = GetConnection(_config))
                    {
                        using (var cmd = new SqlCommand("SELECT id, ISNULL(imo_no, 0), ISNULL(mmsi_no, 0), name FROM TRACK.V_VESSEL", db))
                        {
                            using (var r = cmd.ExecuteReaderWithRetry())
                            {
                                while (r.Read())
                                {
                                    activevessels.Add(new Vessel(Convert.ToInt32(r[0]), Convert.ToInt32(r[1]), Convert.ToInt32(r[2]), r[3].ToString(), _config));
                                }
                            }
                        }
                    }
                    if (activevessels.Count == 0)
                        LoggingService.LogFatal("No vessels for tracking");
                }
                catch (Exception e)
                {
                    LoggingService.LogError("GetVesselsForTracking: " + e);
                    return false;
                }
                foreach (Vessel v in activevessels)
                {
                    Vessel oldv = vessels.Find(f => f.ID == v.ID);
                    if (oldv == null)
                    {
                        vessels.Add(v);
                    }
                    else
                    {
                        oldv.Update(v);
                    }
                }
                //Remove not active vessels from list of vessels
                vessels.RemoveAll(delegate (Vessel v) { return v.IsNew == false; });
                if (vessels.Count() == 0)
                    LoggingService.LogFatal("No vessels to track");
                else
                    LoggingService.LogInformation("Number of vessels to track " + vessels.Count().ToString());
                return true;
            }
            catch (Exception e)
            {
                LoggingService.LogError("RefreshVesselList: " + e);
                return false;
            }
        }

        public static void ReportCellTask(ITrackerConfiguration config)
        {
            Thread.Sleep(1 * 60 * 1000);
            while (true)
            {                
                if (bool.Parse(config.GetValue("EncReportEnabled")))
                {
                    ReportENCCells(config);
                }

                Thread.Sleep(System.Convert.ToInt32(config.GetValue("MainLoopThreadSleepInMilliSec")));
            }
        }

        class ProcessedPosition
        {
            public ProcessedPosition(int vesselID, DateTime timeStamp, bool reported) { VesselID = vesselID; TimeStamp = timeStamp; Reported = reported; }
            public readonly int VesselID;
            public readonly DateTime TimeStamp;
            public readonly bool Reported;
        }
        public static void ReportENCCells(ITrackerConfiguration config)
        {
            try
            {
                List<ProcessedPosition> index = new List<ProcessedPosition>();
                using (SqlConnection connection = GetConnection(config))
                {
                    using (SqlCommand command = new SqlCommand("TRACK.GetUnprocessedPositions", connection) { CommandType = CommandType.StoredProcedure })
                    {
                        command.Parameters.AddWithValue("count", 500);
                        using (SqlDataReader reader = command.ExecuteReaderWithRetry())
                        {
                            while (reader.Read())
                            {
                                int vesselId = reader.GetInt32(reader.GetOrdinal("vessel_id"));
                                DateTime date = reader.GetDateTime(reader.GetOrdinal("position_timestamp"));
                                double lat1 = reader.GetDouble(reader.GetOrdinal("lat1"));
                                double lon1 = reader.GetDouble(reader.GetOrdinal("lon1"));
                                double lat2 = reader.GetDouble(reader.GetOrdinal("lat2"));
                                double lon2 = reader.GetDouble(reader.GetOrdinal("lon2"));
                                DateTime time1 = reader.GetDateTime(reader.GetOrdinal("time1"));
                                DateTime time2 = reader.GetDateTime(reader.GetOrdinal("time2"));
                                double speed1 = reader.IsDBNull(reader.GetOrdinal("speed1")) ? 0 : reader.GetDouble(reader.GetOrdinal("speed1"));
                                double speed2 = reader.IsDBNull(reader.GetOrdinal("speed2")) ? 0 : reader.GetDouble(reader.GetOrdinal("speed2"));
                                int sourceId = reader.GetInt32(reader.GetOrdinal("source_id"));
                                string VesselName = reader["name"].ToString();
                                try
                                {
                                    bool reported = Vessel.Report(config, lat1, lon1, lat2, lon2, time1, time2, speed1,
                                        speed2, (TrackingSource)sourceId, vesselId, VesselName);
                                    index.Add(new ProcessedPosition(vesselId, date, reported));
                                }
                                catch (Exception e)
                                {
                                    LoggingService.LogWarning("Error reporting position {0} for {1}: {2}", date, VesselName, e);
                                }
                            }
                        }
                    }
                    using (SqlCommand command = new SqlCommand("TRACK.SetPositionProcessed", connection) { CommandType = CommandType.StoredProcedure })
                    {
                        var paramVesselId = command.Parameters.Add("vessel_id", SqlDbType.Int);
                        var paramDateId = command.Parameters.Add("position_timestamp", SqlDbType.DateTime);
                        var paramReported = command.Parameters.Add("reported", SqlDbType.Bit);
                        foreach (var idx in index)
                        {
                            paramVesselId.Value = idx.VesselID;
                            paramDateId.Value = idx.TimeStamp;
                            paramReported.Value = idx.Reported;
                            command.ExecuteNonQueryWithRetry();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                LoggingService.LogWarning("ReportENC cells failed: " + e);
            }
        }


        private void UpdateTerminalStopped(int stopid)
        {
            using (SqlConnection connection = GetConnection(_config))
            {
                SqlCommand cmd = connection.CreateCommand();
                cmd.CommandText = "UPDATE TRACK.INMARSAT_C_STOP SET stopped=GETUTCDATE() WHERE id=@stopid";
                cmd.Parameters.AddWithValue("@stopid", stopid);
                int norows = cmd.ExecuteNonQueryWithRetry();
                if (norows == 0)
                {
                    LoggingService.LogFatal("Error updating TRACK.INMARSAT_C_STOP");
                }
                connection.Close();
            }
        }

        private void StopTerminal(int stopid, int dnid, int member_no, int inmarsat_mobile_no, string mail_poll_addr, string userid, string password, InmarsatCTracker inmarsatCTracker)
        {
            if (inmarsatCTracker.SendStopPoll(dnid, member_no, inmarsat_mobile_no, mail_poll_addr, userid, password))
                UpdateTerminalStopped(stopid);
        }

        // Read table TRACK.INMARSAT_C_STOP find dnid and member of terminals to stop and send stop commands
        private void SendInmarstCStopPolls(InmarsatCTracker inmarsatCTracker, string replyToAddr)
        {
            List<Tuple<int, int, int, int, string, string, string>> stops = new List<Tuple<int, int, int, int, string, string, string>>();
            using (SqlConnection connection = GetConnection(_config))
            {
                SqlCommand command = connection.CreateCommand();
                command.CommandText = String.Format("SELECT " +
                                                    "id,dnid,member_no,inmarsat_mobile_no,mail_poll_addr,userid,password " +
                                                    "FROM TRACK.V_INMARSAT_C_STOP " +
                                                    "WHERE stopped IS null AND reply_to_addr=@replyToAddr");
                command.Parameters.AddWithValue("@replyToAddr", replyToAddr);
                using (SqlDataReader reader = command.ExecuteReaderWithRetry())
                    while (reader.Read())
                    {
                        int no = 0;
                        int id = reader.GetInt32(no++);
                        int dnid = reader.GetInt32(no++);
                        int member_no = reader.GetByte(no++);
                        int inmarsat_mobile_no = reader.GetInt32(no++);
                        string mail_poll_addr = reader.GetString(no++);
                        string userid = reader.GetString(no++);
                        string password = reader.GetString(no++);
                        stops.Add(new Tuple<int, int, int, int, string, string, string>(id, dnid, member_no, inmarsat_mobile_no, mail_poll_addr, userid, password));
                    }
                foreach (Tuple<int, int, int, int, string, string, string> t in stops)
                {
                    StopTerminal(t.Item1, t.Item2, t.Item3, t.Item4, t.Item5, t.Item6, t.Item7, inmarsatCTracker);
                }
            }
        }

        private void LogGroupPoll(int dnid, string address)
        {
            using (SqlConnection connection = GetConnection(_config))
            {
                using (SqlCommand cmd = new SqlCommand("TRACK.LogGroupPoll", connection) { CommandType = CommandType.StoredProcedure })
                {
                    cmd.Parameters.AddWithValue("mail_addr", address);
                    cmd.Parameters.AddWithValue("dnid", dnid);
                    cmd.ExecuteNonQueryWithRetry();
                }
            }
        }

        private void SendInmarsatCGroupPolls(DNIDGroupPoll g, InmarsatCTracker inmarsatCTracker, string replyToAddr)
        {

            if (inmarsatCTracker.SendGroupPoll(g.DNID, g.PollToAddr, g.UserId, g.Password))
            {
                LogGroupPoll(g.DNID, replyToAddr);
                LoggingService.LogDebug("Successfully sent group poll for DNID " + g.DNID.ToString());
            }
            else
                LoggingService.LogFatal("Error sending inmarsat-C group poll for DNID " + g.DNID.ToString() + " address " + g.PollToAddr);
        }

        private DNIDGroupPoll GetDNIDToPoll(string replyToAddr)
        {
            using (SqlConnection connection = GetConnection(_config))
            {
                using (SqlCommand cmd = new SqlCommand("TRACK.GetNextPollDNID", connection) { CommandType = CommandType.StoredProcedure })
                {
                    cmd.Parameters.AddWithValue("mail_addr", replyToAddr);
                    var poll_to_addr = cmd.Parameters.Add(new SqlParameter("poll_to_addr", SqlDbType.VarChar, 50) { Direction = ParameterDirection.Output });
                    var userid = cmd.Parameters.Add(new SqlParameter("userid", SqlDbType.VarChar, 50) { Direction = ParameterDirection.Output });
                    var password = cmd.Parameters.Add(new SqlParameter("password", SqlDbType.VarChar, 50) { Direction = ParameterDirection.Output });
                    var returnParam = cmd.Parameters.Add(new SqlParameter("", SqlDbType.Int) { Direction = ParameterDirection.ReturnValue });
                    cmd.ExecuteNonQueryWithRetry();
                    int dnid = (int)returnParam.Value;
                    if (dnid == -1)
                        return null;
                    return new DNIDGroupPoll(dnid, (string)poll_to_addr.Value, replyToAddr, (string)userid.Value, (string)password.Value);
                }
            }
        }

        List<DNIDRequest> GetDNIDsToSinglePoll(string mailAddr)
        {
            List<DNIDRequest> toPoll = new List<DNIDRequest>();
            using (SqlConnection connection = GetConnection(_config))
            {
                SqlCommand command = connection.CreateCommand();
                command.CommandText = String.Format("SELECT vessel_id,dnid,member_no,inmarsat_mobile_no,ocean_region,mail_poll_addr,userid,password FROM TRACK.V_INMARSAT_C_VESSELS_TO_POLL WHERE reply_to_addr=@mailAddr");
                command.Parameters.AddWithValue("@mailAddr", mailAddr);
                using (SqlDataReader reader = command.ExecuteReaderWithRetry())
                {
                    while (reader.Read())
                    {
                        int idx = 0;
                        int vessel_id = reader.GetInt32(idx++);
                        int dnid = reader.GetInt32(idx++);
                        int member_no = (int)reader.GetByte(idx++);
                        int inmarsat_mobile_no = reader.GetInt32(idx++);
                        int ocean_region = reader.GetInt32(idx++);
                        string mail_poll_addr = reader.GetString(idx++);
                        string userid = reader.GetString(idx++);
                        string password = reader.GetString(idx++);
                        toPoll.Add(new DNIDRequest(vessel_id, dnid, member_no, inmarsat_mobile_no, ocean_region, mail_poll_addr, userid, password));
                    }
                }
                return toPoll;
            }
        }

        private void SendSinglePolls(InmarsatCTracker inmarsatCTracker, string mailAddr)
        {
            List<DNIDRequest> requests = GetDNIDsToSinglePoll(mailAddr);
            if (requests.Count() > 0)
                inmarsatCTracker.SinglePoll(requests, mailAddr);
        }

        private bool PollInmarsatC(string mailAddr)
        {
            try
            {
                using (InmarsatCTracker inmarsatCTracker = new InmarsatCTracker(mailAddr, _config, GetToken(_config)))
                {
                    DNIDGroupPoll g = GetDNIDToPoll(mailAddr);
                    if (g != null)
                        SendInmarsatCGroupPolls(g, inmarsatCTracker, mailAddr);
                    SendInmarstCStopPolls(inmarsatCTracker, mailAddr);
                    if (DateTime.UtcNow > singlePollStart)
                        SendSinglePolls(inmarsatCTracker, mailAddr);
                }
            }
            catch (Exception e)
            {
                LoggingService.LogFatal("PollInmarsatC " + e);
                return false;
            }
            return true;
        }

        public static List<string> GetInmarsatCMailToAddrs(ITrackerConfiguration config)
        {
            List<string> mailToAdds = new List<string>();
            using (SqlConnection connection = GetConnection(config))
            {
                SqlCommand command = connection.CreateCommand();
                command.CommandText = String.Format("SELECT DISTINCT dnid_email FROM TRACK.V_INMARSAT_C WHERE active=1");
                using (SqlDataReader reader = command.ExecuteReaderWithRetry())
                    while (reader.Read())
                    {
                        string s = reader.GetString(0);
                        mailToAdds.Add(s);
                    }
            }
            return mailToAdds;
        }

        //Add positions collected by independent threads to 
        public int ProcessPositionsRaw()
        {
            try
            {

                using (var n = new PositionRawReader(_config.GetValue("SQLAzureConnectionString")))
                {
                    long msRead, msProcess, msDelete, msPos, msDest, msNav;
                    var w = new Stopwatch();
                    w.Start();
                    var pos = n.Read();
                    w.Stop(); msRead = w.ElapsedMilliseconds; w.Restart();
                    DateTime startProc = DateTime.UtcNow;
                    //LoggingService.LogInformation("Processing " + pos.Count + " positions from TRACK.POSITION_RAW");
                    using (SqlConnection connection = GetConnection(_config))
                    {
                        Stopwatch wPos = new Stopwatch(), wDest = new Stopwatch(), wNav = new Stopwatch();
                        DataTable dtNav = new DataTable("VesselsNavStatus");
                        dtNav.Columns.Add("vessel_id", typeof(int)); dtNav.Columns.Add("timestamp", typeof(DateTime)); dtNav.Columns.Add("source_id", typeof(byte));
                        dtNav.Columns.Add("navstatus_id", typeof(byte));
                        DataTable dtDest = new DataTable("VesselsDestination");
                        dtDest.Columns.Add("vessel_id", typeof(int)); dtDest.Columns.Add("timestamp", typeof(DateTime)); dtDest.Columns.Add("source_id", typeof(byte));
                        dtDest.Columns.Add("destination", typeof(string)); dtDest.Columns.Add("eta", typeof(DateTime));
                        DataTable dtPos = new DataTable("VesselsPositionTable");
                        dtPos.Columns.Add("vessel_id", typeof(int)); dtPos.Columns.Add("timestamp", typeof(DateTime)); dtPos.Columns.Add("source_id", typeof(byte));
                        dtPos.Columns.Add("latitude", typeof(decimal)); dtPos.Columns.Add("longitude", typeof(decimal)); dtPos.Columns.Add("speed", typeof(decimal)); dtPos.Columns.Add("course", typeof(decimal));
                        foreach (var p in pos.Values)
                        {
                            Vessel v = null;
                            v = vessels.Find(f => f.ID == p.vessel_id);
                            if (v != null)
                            {
                                if (!p.isDestination)
                                {
                                    if (v.ShouldLog(p.lat, p.lon, p.timeStamp, p.source, p.isMainSource,
                                                     p.isInOpArea ?? false, p.lastLoggedTimestamp, p.lastLoggedLat, p.lastLoggedLon,
                                                     ref p.speed, ref p.heading))
                                        dtPos.Rows.Add(p.vessel_id, p.timeStamp, (byte)(int)p.source, p.lat, p.lon, p.speed, p.heading);
                                }
                                else if (!string.IsNullOrEmpty(p.dest) && p.eta != null)
                                {
                                    dtDest.Rows.Add(p.vessel_id, p.timeStamp, (byte)(int)p.source, p.dest, p.eta);
                                }
                                if (p.NavStatus != null)
                                {
                                    dtNav.Rows.Add(p.vessel_id, p.timeStamp, (byte)(int)p.source, (byte)(p.NavStatus ?? 0));
                                }
                            }
                        }
                        wPos.Start();
                        using (var cmd = new SqlCommand("TRACK.AddVesselsPosition", connection) { CommandType = CommandType.StoredProcedure })
                        {
                            cmd.Parameters.AddWithValue("vessels", dtPos).SqlDbType = SqlDbType.Structured;
                            cmd.ExecuteNonQuery();
                        }
                        wPos.Stop();
                        wDest.Start();
                        using (var cmd = new SqlCommand("TRACK.SetVesselsDestination", connection) { CommandType = CommandType.StoredProcedure })
                        {
                            cmd.Parameters.AddWithValue("vessels", dtDest).SqlDbType = SqlDbType.Structured;
                            cmd.ExecuteNonQuery();
                        }
                        wDest.Stop();
                        wNav.Start();
                        using (var cmd = new SqlCommand("TRACK.SetVesselsNavStatus", connection) { CommandType = CommandType.StoredProcedure })
                        {
                            cmd.Parameters.AddWithValue("vessels", dtNav).SqlDbType = SqlDbType.Structured;
                            cmd.ExecuteNonQuery();
                        }
                        wNav.Stop();
                        msPos = wPos.ElapsedMilliseconds;
                        msDest = wDest.ElapsedMilliseconds;
                        msNav = wNav.ElapsedMilliseconds;
                    }
                    w.Stop();
                    msProcess = w.ElapsedMilliseconds;
                    w.Restart();
                    n.DeletePositions(pos.Keys);
                    w.Stop();
                    msDelete = w.ElapsedMilliseconds;
                    double elapsed = DateTime.UtcNow.Subtract(startProc).TotalSeconds;
                    LoggingService.LogInformation($"PositionRaw #{pos.Count()} Start {startProc} time {elapsed} seconds ({msRead}/{msProcess}/{msDelete}) ({msPos}/{msDest}/{msNav})");
                    return pos.Count();
                }
            }
            catch (Exception e)
            {
                LoggingService.LogWarning("PositionRawReader: " + e);
                return -1;
            }
        }

        public void UpdateVesselPositions()
        {
            bool bHeartbeat = int.Parse(_config.GetValue("Heartbeat")) != 0;
            if (bHeartbeat)
                LoggingService.LogDebug("VesselTracker: UpdateVesselPositions ({0} vessels)", vessels.Count);
#if (!DEBUG)
                
            if (DateTime.UtcNow > startPositionRaw && bool.Parse(_config.GetValue("PositionRawEnabled")))
            {
                int noPosProcessed = ProcessPositionsRaw();
                if (noPosProcessed >= 0)
                {
                    delayPositionRaw = 0;
                    if (noPosProcessed < 50)
                        startPositionRaw = DateTime.UtcNow.AddSeconds(5); //Sleep 5 seconds in case TRACK.POSITION_RAW is close to empty
                    else
                        startPositionRaw = DateTime.UtcNow; 
                }
                else
                {
                    delayPositionRaw = (delayPositionRaw + 0.1) * 2;
                    if (delayPositionRaw > 30)
                        LoggingService.LogFatal(string.Format("Repeated error in PositionRaw processing. Delaying service by {0} minutes", delayPositionRaw));
                    else
                        LoggingService.LogError(string.Format("Error in PositionRaw processing. Delaying service by {0} minutes", delayPositionRaw));
                    startPositionRaw = DateTime.UtcNow.AddMinutes(delayPositionRaw);
                }
            }
#endif

            //InmarsatC
            if (DateTime.UtcNow > startInmarsatCTime && bool.Parse(_config.GetValue("InmarsatEnabled")))
            { //Add progressive delay in case of exception....
                if (bHeartbeat)
                    LoggingService.LogDebug("VesselTracker: UpdateVesselPositions: InmarsatC");

                bool ok = true;
                List<string> inmarsatCmails = new List<string>();
                try
                {
                    inmarsatCmails = GetInmarsatCMailToAddrs(_config);
                }
                catch (Exception e)
                {
                    LoggingService.LogError("Error reading mail adresses" + e);
                    ok = false;
                }

                if (ok)
                {
#if !DEBUG
                    foreach (string mailAddr in inmarsatCmails)
                        if (!PollInmarsatC(mailAddr))
                            ok = false;
#endif
                }
                if (ok)
                {
                    delayInmarsatC = 0;
                    startInmarsatCTime = DateTime.UtcNow.AddMinutes(5);
                }
                else
                {
                    delayInmarsatC = (delayInmarsatC + 0.1) * 2;
                    if (delayInmarsatC > 0.3)
                        LoggingService.LogFatal(String.Format("Repeated error in InmarsatC Polling delaying service by {0} hours", delayInmarsatC));
                    else
                        LoggingService.LogError(String.Format("Error in InmarsatC Polling delaying service by {0} hours", delayInmarsatC));
                    startInmarsatCTime = DateTime.UtcNow.AddHours(delayInmarsatC);
                }
            }
        }

        private static string GetToken(ITrackerConfiguration config)
        {
            try
            {
                string clientId = config.GetValue("TrackingBackEndAppClientId");
                string clientSecret = config.GetValue("TrackingBackEndAppClientSecret");
                string instance = config.GetValue("TrackingBackEndAppInstance");
                string tenant = config.GetValue("TrackingBackEndAppTenant");

                string authority = String.Format(CultureInfo.InvariantCulture, instance, tenant);
                IConfidentialClientApplication clientApplication = ConfidentialClientApplicationBuilder.Create(clientId)
                    .WithClientSecret(clientSecret)
                    .WithAuthority(authority)
                    .Build();
                string[] scopes =
                {
                    "https://outlook.office365.com/.default"
                };
                AuthenticationResult result = clientApplication.AcquireTokenForClient(scopes).ExecuteAsync().GetAwaiter().GetResult();
                return result.AccessToken;
            }
            catch (Exception ex)
            {
                LoggingService.LogError($"Error while getting a token: {ex.Message}");
                return null;
            }
        }

        #region Transient error-handling
        private static SqlConnection GetConnection(ITrackerConfiguration config)
        {
            SqlConnection db = new SqlConnection(config.GetValue("SQLAzureConnectionString"));
            db.OpenWithRetry();
            return db;
        }
        private static SqlConnection GetConnectionErp(ITrackerConfiguration config)
        {
            SqlConnection db = new SqlConnection(config.GetValue("ErpConnectionString"));
            db.OpenWithRetry();
            return db;
        }
        #endregion
    }
}