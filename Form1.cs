namespace HelloWorld;

public partial class Form1 : Form
{
    // Animation fields
    private System.Windows.Forms.Timer animationTimer;
    private int animationFrame;

    public Form1()
    {
        InitializeComponent();
        animationFrame = 0;
        animationTimer = new System.Windows.Forms.Timer();
        animationTimer.Interval = 100;
        animationTimer.Tick += animationTimer_Tick;
    }

    private void btnDrink_Click(object sender, EventArgs e)
    {
        animationFrame = 0;
        animationTimer.Start();
    }

    private void animationTimer_Tick(object? sender, EventArgs e)
    {
        animationFrame++;
        if (animationFrame > 10)
        {
            animationTimer.Stop();
        }
        pictureBoxMan.Invalidate();
    }

    private void pictureBoxMan_Paint(object sender, PaintEventArgs e)
    {
        var g = e.Graphics;
        Pen pen = Pens.Black;
        int centerX = pictureBoxMan.Width / 2;
        int baseY = pictureBoxMan.Height - 40;
        // Head
        g.DrawEllipse(pen, centerX - 20, baseY - 120, 40, 40);
        // Body
        g.DrawLine(pen, centerX, baseY - 80, centerX, baseY - 20);
        // Arms
        g.DrawLine(pen, centerX, baseY - 70, centerX - 30, baseY - 50);
        // Only draw the right arm (holding the mug) if not animating
        if (animationFrame == 0)
        {
            g.DrawLine(pen, centerX, baseY - 70, centerX + 30, baseY - 50);
        }
        // Legs
        g.DrawLine(pen, centerX, baseY - 20, centerX - 20, baseY);
        g.DrawLine(pen, centerX, baseY - 20, centerX + 20, baseY);
        // Beer mug (simple yellow rectangle)
        // Animate mug moving from hand to mouth (diagonally up and left)
        int mugStartX = centerX + 30;
        int mugStartY = baseY - 60;
        int mugEndX = centerX - 10;
        int mugEndY = baseY - 120 + 20; // near mouth
        float t = animationFrame / 10f;
        int mugX = (int)(mugStartX + (mugEndX - mugStartX) * t);
        int mugY = (int)(mugStartY + (mugEndY - mugStartY) * t);
        g.FillRectangle(Brushes.Gold, mugX, mugY, 15, 25);
        g.DrawRectangle(pen, mugX, mugY, 15, 25);
        // Arm holding mug (animated)
        g.DrawLine(pen, centerX, baseY - 70, mugX + 7, mugY + 12);

        // Speakerbox during animation
        if (animationFrame > 0 && animationFrame <= 10)
        {
            string text = "Fy faen så deiligt!";
            var font = new Font("Arial", 14, FontStyle.Bold);
            var textSize = g.MeasureString(text, font);
            int boxWidth = (int)textSize.Width + 20;
            int boxHeight = (int)textSize.Height + 20;
            int boxX = centerX - boxWidth / 2;
            int boxY = baseY - 160;
            g.FillRectangle(Brushes.WhiteSmoke, boxX, boxY, boxWidth, boxHeight);
            g.DrawRectangle(Pens.Black, boxX, boxY, boxWidth, boxHeight);
            g.DrawString(text, font, Brushes.Black, boxX + 10, boxY + 10);
        }
    }
}
