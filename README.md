1. Clone & Navigate
powershell
git clone https://github.com/ybinfotech2521/SQL_GeneratorBOT.git
cd SQL_GeneratorBOT
cd backend
2. Setup Virtual Environment
powershell
# Create virtual environment
python -m venv venv

# Activate it
venv\Scripts\Activate.ps1

# If you get execution policy error, run:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
# Then activate again
3. Install Dependencies
powershell
pip install -r requirements.txt
4. Configure Environment
powershell
# Copy template
Copy-Item .env.example .env
Edit .env with your:

PostgreSQL credentials

Groq API key from console.groq.com

5. Run the Backend Server
powershell
# Start FastAPI server
uvicorn app.main:app --reload --port 8000

# You'll see:
# INFO:     Uvicorn running on http://127.0.0.1:8000
# INFO:     Application startup complete.
6. Open Frontend
Open frontend/index.html in your browser
OR
Go to http://localhost:8000/docs for interactive API documentation
