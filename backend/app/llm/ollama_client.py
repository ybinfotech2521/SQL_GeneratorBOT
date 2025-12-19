# backend/app/llm/ollama_client.py
"""
Ollama client for local LLM inference.
No API keys, no rate limits.
"""

import os
import aiohttp
import json
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

async def call_ollama_chat(
    messages: List[Dict[str, Any]],
    model: Optional[str] = None,
    max_tokens: int = 1024,
    temperature: float = 0.0,
    stop: Optional[List[str]] = None,
) -> str:
    """
    Send chat request to local Ollama server.
    
    messages: list of {"role": "system"|"user"|"assistant", "content": "..."}
    Returns the generated text.
    """
    model_name = model or OLLAMA_MODEL
    url = f"{OLLAMA_BASE_URL}/api/chat"
    
    # Format messages for Ollama's API
    formatted_messages = []
    for msg in messages:
        formatted_messages.append({"role": msg["role"], "content": msg["content"]})
    
    payload = {
        "model": model_name,
        "messages": formatted_messages,
        "stream": False,
        "options": {
            "num_predict": max_tokens,
            "temperature": temperature,
        }
    }
    
    # Add stop sequences if provided
    if stop:
        payload["options"]["stop"] = stop
    
    timeout = aiohttp.ClientTimeout(total=120)  # 2 minute timeout
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("message", {}).get("content", "").strip()
                else:
                    error_text = await response.text()
                    raise RuntimeError(f"Ollama API error {response.status}: {error_text}")
        except aiohttp.ClientConnectorError:
            raise RuntimeError(f"Cannot connect to Ollama at {OLLAMA_BASE_URL}. Is Ollama running?")
        except Exception as e:
            raise RuntimeError(f"Ollama request failed: {e}")

# Simple test function
async def test_ollama():
    """Test connection to Ollama."""
    messages = [
        {"role": "system", "content": "You are a helpful SQL expert."},
        {"role": "user", "content": "What is 2+2? Answer in one word."}
    ]
    try:
        response = await call_ollama_chat(messages, max_tokens=10)
        print(f"✅ Ollama test successful! Response: '{response}'")
        return True
    except Exception as e:
        print(f"❌ Ollama test failed: {e}")
        print("\n🔧 TROUBLESHOOTING:")
        print("1. Make sure Ollama application is RUNNING")
        print("2. In a separate terminal, run: ollama run llama3.2:3b")
        print("3. Verify the model is downloaded: ollama list")
        return False

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_ollama())