# backend/app/llm/groq_client.py
"""
Groq Cloud API client (correct implementation for your API key)
"""

import os
import httpx
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

# CORRECT GROQ API SETTINGS
GROQ_API_URL = os.getenv("GROQ_API_URL", "https://api.groq.com/openai/v1/chat/completions")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

# Headers for Groq API
HEADERS = {
    "Authorization": f"Bearer {GROQ_API_KEY}",
    "Content-Type": "application/json"
} 

async def call_groq_chat(
    messages: List[Dict[str, Any]],
    model: Optional[str] = None,
    max_tokens: int = 1024,
    temperature: float = 0.0,
    stop: Optional[List[str]] = None
) -> str:
    """
    Call Groq Cloud API (OpenAI-compatible endpoint)
    
    messages: list of {"role": "system"|"user"|"assistant", "content": "..."}
    Returns the assistant's text response
    """
    model_name = model or GROQ_MODEL
    
    payload = {
        "model": model_name,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "stream": False
    }
    
    if stop:
        payload["stop"] = stop
    
    timeout = httpx.Timeout(60.0, connect=10.0)
    
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            response = await client.post(GROQ_API_URL, json=payload, headers=HEADERS)
            
            if response.status_code == 200:
                data = response.json()
                # Extract response from OpenAI-compatible format
                if "choices" in data and len(data["choices"]) > 0:
                    return data["choices"][0]["message"]["content"].strip()
                else:
                    return str(data)
                    
            elif response.status_code == 401:
                error_msg = "Invalid Groq API key. Check your .env file."
            elif response.status_code == 429:
                error_msg = "Groq rate limit exceeded. Free tier has limits."
            elif response.status_code == 404:
                error_msg = f"Model '{model_name}' not found. Available models: llama-3.1-8b-instant, llama-3.2-3b-text, mixtral-8x7b-32768"
            else:
                error_msg = f"Groq API error {response.status_code}: {response.text[:200]}"
                
            raise RuntimeError(error_msg)
            
        except httpx.ConnectError:
            raise RuntimeError("Cannot connect to Groq API. Check internet connection.")
        except Exception as e:
            raise RuntimeError(f"Groq API request failed: {e}")

async def test_groq():
    """Test connection to Groq API"""
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is 2+2? Answer in one word."}
    ]

    try:
        response = await call_groq_chat(messages, max_tokens=10)
        print(f"✅ Groq API test successful! Response: '{response}'")
        return True
    except Exception as e:
        print(f"❌ Groq API test failed: {e}")
        print("\n🔧 TROUBLESHOOTING:")
        print(f"1. Check API key: {GROQ_API_KEY[:10]}...")
        print(f"2. Check endpoint: {GROQ_API_URL}")
        print(f"3. Check model: {GROQ_MODEL}")
        print("4. Visit https://console.groq.com to verify your key")
        return False

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_groq())