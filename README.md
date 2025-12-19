
Step 1: Clone & Open Project
bash
git clone https://github.com/ybinfotech2521/SQL_GeneratorBOT.git

Step 2: Setup Virtual Environment (Command Prompt)
open cmd from folder
cd backend
python -m venv venv
venv\Scripts\activate
🔴 IMPORTANT: You must use Command Prompt for this step.

Step 3: Install Dependencies
bash
# Still in Command Prompt with (venv) active
pip install -r requirements.txt

Step 4: Configure Environment
bash
# Copy the template
copy .env.example .env

# Edit .env file with:
# 1. Your PostgreSQL database credentials
# 2. Your Groq API key from console.groq.com


Step 5: Start the Server (PowerShell)
Open a new PowerShell window

Navigate to the backend:

cd SQL_GeneratorBOT\backend
Activate environment:

venv\Scripts\Activate.ps1
Start the server:

uvicorn app.main:app --reload --port 8000
✅ Server is now running at: http://localhost:8000

Step 6: Open Frontend
Open frontend/index.html in your browser.

ask the question and for the query see the powershell logs 
---------------------------------------------------------------------------------------

<!-- in powershell 
cd "E:\ecom-llm-analytics\backend"

# activate the venv (use .bat if .ps1 blocked)
E:\ecom-llm-analytics\venv\Scripts\activate.ps1

# start uvicorn (run in foreground so you can see logs)
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000 -->
