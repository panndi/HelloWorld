namespace HelloWorld;

partial class Form1
{
    /// <summary>
    ///  Required designer variable.
    /// </summary>
    private System.ComponentModel.IContainer components = null;

    /// <summary>
    ///  Clean up any resources being used.
    /// </summary>
    /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
    protected override void Dispose(bool disposing)
    {
        if (disposing && (components != null))
        {
            components.Dispose();
        }
        base.Dispose(disposing);
    }

    #region Windows Form Designer generated code

    /// <summary>
    ///  Required method for Designer support - do not modify
    ///  the contents of this method with the code editor.
    /// </summary>
    private void InitializeComponent()
    {
        components = new System.ComponentModel.Container();
        AutoScaleMode = AutoScaleMode.Font;
        ClientSize = new Size(800, 450);
        Text = "Danagjengen Virtual Gladmat Reallity";

        btnDrink = new System.Windows.Forms.Button();
        btnDrink.Text = "Drink Beer";
        btnDrink.Location = new System.Drawing.Point(350, 380);
        btnDrink.Size = new System.Drawing.Size(100, 40);
        btnDrink.Click += btnDrink_Click;
        Controls.Add(btnDrink);

        pictureBoxMan = new System.Windows.Forms.PictureBox();
        pictureBoxMan.Location = new System.Drawing.Point(250, 50);
        pictureBoxMan.Size = new System.Drawing.Size(300, 300);
        pictureBoxMan.Paint += pictureBoxMan_Paint;
        pictureBoxMan.BackColor = System.Drawing.Color.White;
        Controls.Add(pictureBoxMan);
    }

    #endregion

    private System.Windows.Forms.Button btnDrink;
    private System.Windows.Forms.PictureBox pictureBoxMan;
}
