# backend/test_groq_simple.py
import asyncio
import sys
import os
from dotenv import load_dotenv

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

async def test():
    print("🧪 Testing Groq API connection...")
    
    # First, check if API key is set
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("❌ GROQ_API_KEY is not set in .env file")
        return
    
    print(f"✓ API Key found: {api_key[:10]}...")
    
    # Try to import and test
    try:
        from app.llm.groq_client import test_groq
        success = await test_groq()
        if success:
            print("\n✅ Groq API is working correctly!")
            print("Your e-commerce chatbot will now use Groq for SQL generation.")
        else:
            print("\n❌ Groq API test failed. Check the error above.")
    except ImportError as e:
        print(f"❌ Cannot import groq_client: {e}")
        print("Make sure you created the file at: backend/app/llm/groq_client.py")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    asyncio.run(test())