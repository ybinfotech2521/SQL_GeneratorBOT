🛒 E-commerce AI Analytics Chatbot
An intelligent chatbot that analyzes your e-commerce database. Ask questions in plain English and get data-driven answers with generated SQL.

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
Frontend Setup
bash
# Open frontend/index.html in browser
# OR use the API at http://localhost:8000/docs
⚙️ Configuration
.env File Setup
env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_db
DB_USER=postgres
DB_PASSWORD=your_password

# Groq API (Get key: https://console.groq.com)
GROQ_API_URL=https://api.groq.com/openai/v1/chat/completions
GROQ_API_KEY=your_key_here
GROQ_MODEL=llama-3.1-8b-instant

# App Settings
MAX_QUERY_ROWS=1000
📁 Project Structure
text
backend/
├── app/
│   ├── llm/              # AI integration (SQL generation, answers)
│   ├── db/               # Database connection
│   ├── routes/           # API endpoints
│   └── utils/            # Schema loading, safety checks
├── requirements.txt      # Python dependencies
└── .env                  # Configuration
frontend/
└── index.html           # Web interface
🔧 How It Works
User asks a question → "Show me top customers"

AI generates SQL → Uses your database schema

Query executes → Safe, read-only database access

AI formats answer → Natural language response

🎯 Test Your Setup
bash
# Test database connection
python test_pipeline.py

# Test AI integration
python test_groq_simple.py
🚨 Troubleshooting
Database issues? Check PostgreSQL is running

AI API errors? Verify Groq API key at console.groq.com

Frontend not loading? Ensure backend is running on port 8000

📞 API Usage
bash
# Example API call
curl -X POST "http://localhost:8000/api/query" \
  -H "Content-Type: application/json" \
  -d '{"userQuery": "What was total revenue last month?"}'
Server running? Visit http://localhost:8000 for status.

Your chatbot is ready to analyze e-commerce data! 🎉



cd "E:\ecom-llm-analytics\backend"

# activate the venv (use .bat if .ps1 blocked)
E:\ecom-llm-analytics\venv\Scripts\activate.ps1

# start uvicorn (run in foreground so you can see logs)
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
