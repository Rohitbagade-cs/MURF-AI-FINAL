from xmlrpc import client
from fastapi import FastAPI, Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi import UploadFile, File
import google.generativeai as genai
from google.generativeai import protos
import assemblyai as aai
import requests
import os, time, secrets, logging
from datetime import datetime
import json
from dotenv import load_dotenv
import uuid
from typing import Dict, List, Set
import websockets
import base64
import asyncio
import subprocess
import wave
import logging
from typing import Type
from assemblyai.streaming.v3 import (
    BeginEvent,
    StreamingClient,
    StreamingClientOptions,
    StreamingError,
    StreamingEvents,
    StreamingParameters,
    StreamingSessionParameters,
    TerminationEvent,
    TurnEvent,
)

# Load environment variables
load_dotenv()

# API Keys
API_KEY = "8702ab13aac045dcbd04ff2441f72877"
MURF_API_KEY = os.getenv("MURF_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
WEATHER_KEY = os.getenv("WEATHER_KEY")
SEARCH_API_KEY = os.getenv("SEARCH_API_KEY")  # New: Add to your .env file
SEARCH_ENGINE_ID = os.getenv("SEARCH_ENGINE_ID")  # New: Add to your .env file

# Configure Gemini AI
genai.configure(api_key=GEMINI_API_KEY)

# Configure AssemblyAI
aai.settings.api_key = os.getenv("ASSEMBLYAI_API_KEY")

app = FastAPI()

# Persona catalog with enhanced system prompts
PERSONAS = {
    "neutral": {
        "system": (
            "You are a helpful, concise assistant with access to weather information and web search. "
            "Keep responses short, clear, and friendly. Use your tools when needed to provide accurate information."
        ),
        "voice": {"voiceId": "en-IN-isha", "style": "Conversational", "rate": 0, "pitch": 0}
    },
    "pirate": {
        "system": (
            "You are a cheerful pirate with access to weather and web search tools. Sprinkle in pirate slang like 'Arrr', 'matey', "
            "and 'yo-ho-ho', but keep answers genuinely helpful and use your tools when the landlubbers need information!"
        ),
        "voice": {"voiceId": "en-IN-isha", "style": "Conversational", "rate": -2, "pitch": -2}
    },
    "cowboy": {
        "system": (
            "You are a friendly cowboy from the old West with modern tools at your disposal. Use down-to-earth language, "
            "sayings like 'howdy' and 'partner', while staying helpful and using weather/search tools when needed."
        ),
        "voice": {"voiceId": "en-IN-isha", "style": "Conversational", "rate": -1, "pitch": -1}
    },
    "robot": {
        "system": (
            "You are a polite robot with weather and web search capabilities. Speak with precise, structured sentences, "
            "occasionally using phrases like 'Processing' or 'Directive acknowledged'â€”but remain warm and helpful."
        ),
        "voice": {"voiceId": "en-IN-isha", "style": "Conversational", "rate": +1, "pitch": +2}
    },
}

# Storage
session_persona: Dict[str, str] = {}
chat_history: Dict[str, List[Dict[str, str]]] = {}
murf_ws_clients: Set[WebSocket] = set()

def get_persona(session_id: str) -> dict:
    pid = session_persona.get(session_id, "neutral")
    return PERSONAS.get(pid, PERSONAS["neutral"])

# --- ENHANCED SKILLS ---

def get_weather(city: str) -> str:
    """Get current weather for a city"""
    try:
        if not WEATHER_KEY:
            return "Weather service is not configured. Please check API keys."
            
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={WEATHER_KEY}&units=metric"
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            return f"Sorry, I couldn't find weather information for {city}. Please check the city name."
            
        data = response.json()
        
        temp = data['main']['temp']
        feels_like = data['main']['feels_like']
        description = data['weather'][0]['description'].title()
        humidity = data['main']['humidity']
        wind_speed = data['wind']['speed']
        pressure = data['main']['pressure']
        
        return (
            f"Current weather in {city}: {temp}Â°C (feels like {feels_like}Â°C), {description}. "
            f"Humidity: {humidity}%, Wind: {wind_speed} m/s, Pressure: {pressure} hPa."
        )
    except requests.exceptions.Timeout:
        return "Weather service is temporarily unavailable. Please try again later."
    except Exception as e:
        print(f"Weather API error: {e}")
        return f"Error fetching weather data for {city}. Please try again."

def search_web(query: str, num_results: int = 3) -> str:
    """Search the web using Google Custom Search API"""
    try:
        if not SEARCH_API_KEY or not SEARCH_ENGINE_ID:
            print("ðŸ” Missing API keys")  # ADD THIS TOO
            return "Web search is not configured. Please check API keys."
        
        url = "https://www.googleapis.com/customsearch/v1"
        params = {
            'key': SEARCH_API_KEY,
            'cx': SEARCH_ENGINE_ID,
            'q': query.strip(),  # Clean the query
            'num': int(num_results),  # ADD int() conversion here
            'safe': 'active'  # Add safe search
        }
        # ðŸ” Debug log BEFORE the API call
        print(f"Making search request with query: '{query}' (length: {len(query)})")
        print(f"Search URL: {url}")
        print(f"ðŸ” Search request: {params}")  # Debug log

        response = requests.get(url, params=params, timeout=10)

        print(f"ðŸ” Search response status: {response.status_code}")  # Debug 
        print(f"Response content: {response.text[:100]}...")  # First 100 chars
        
        if response.status_code == 400:
            # Get the full error message
            try:
                error_data = response.json()
                print(f"Full error response: {error_data}")
                error_message = error_data.get('error', {}).get('message', 'Unknown error')
                return f"Search API error: {error_message}"
            except:
                return f"Search API returned 400 but couldn't parse error"
        if response.status_code != 200:
            print(f"ðŸ” Search failed with status: {response.status_code}")
            return "Search service is temporarily down. Please try again later."
        
        data = response.json()
        
        if 'items' not in data or not data['items']:
            return f"No search results found for: {query}"
        
        results = []
        for item in data['items'][:num_results]:
            title = item.get('title', 'No title')
            snippet = item.get('snippet', 'No description available')
            link = item.get('link', '')
            results.append(f"â€¢ {title}: {snippet}")
        
        search_summary = f"Here are the top search results for '{query}':\n\n" + "\n\n".join(results)
        
        # Limit response length for TTS
        if len(search_summary) > 500:
            search_summary = search_summary[:500] + "... (showing first few results)"
            
        return search_summary
        
    except requests.exceptions.Timeout:
        return "Search service is temporarily unavailable. Please try again later."
    except Exception as e:
        print(f"Search exception: {str(e)}")
        return f"Search failed: {str(e)}"

def get_news(topic: str = "latest", num_results: int = 3) -> str:
    """Get latest news with better context handling"""
    print(f"ðŸ“° get_news called with topic: '{topic}', num_results: {num_results}")  # ADD THIS LINE
    try:
        topic_clean = topic.lower().strip()
        
        # Better topic mapping
        if topic_clean in ["india", "in india", "about india"]:
            query = "latest news headlines India today"
        elif topic_clean in ["latest", "news", "today", "headlines"]:
            query = "latest news headlines today"
        elif "headlines" in topic_clean:
            query = f"latest {topic} headlines"
        else:
            query = f"{topic} news today"
        
        print(f"News query being sent to search: '{query}'")  # Debug log
        result = search_web(query, num_results)
        print(f"News search result: {result[:100]}...")  # Debug log
        
        return result
        
    except Exception as e:
        print(f"News error: {e}")
        return "Unable to fetch news right now. Please try again later."
# Enhanced Gemini model with all tools
def get_gemini_model_with_tools():
    """Get Gemini model with weather, web search, and news tools"""
    
    weather_function = protos.FunctionDeclaration(
        name="get_weather",
        description="Get current weather information for any city worldwide",
        parameters=protos.Schema(
            type=protos.Type.OBJECT,
            properties={
                "city": protos.Schema(
                    type=protos.Type.STRING, 
                    description="City name (e.g., 'London', 'New York', 'Tokyo')"
                )
            },
            required=["city"]
        ),
    )
    
    search_function = protos.FunctionDeclaration(
        name="search_web",
        description="Search the web for current information, facts, or answers to questions",
        parameters=protos.Schema(
            type=protos.Type.OBJECT,
            properties={
                "query": protos.Schema(
                    type=protos.Type.STRING, 
                    description="Search query or question"
                ),
                "num_results": protos.Schema(
                    type=protos.Type.INTEGER,
                    description="Number of results to return (1-5, default 3)"
                )
            },
            required=["query"]
        ),
    )
    
    news_function = protos.FunctionDeclaration(
        name="get_news",
        description="Get the latest news headlines or news about a specific topic",
        parameters=protos.Schema(
            type=protos.Type.OBJECT,
            properties={
                "topic": protos.Schema(
                    type=protos.Type.STRING, 
                    description="News topic or 'latest' for general news"
                ),
                "num_results": protos.Schema(
                    type=protos.Type.INTEGER,
                    description="Number of news items to return (1-5, default 3)"
                )
            },
            required=["topic"]
        ),
    )
    
    return genai.GenerativeModel(
        model_name="gemini-1.5-flash",  # Changed from Pro to Flash for better quota limits
        tools=[protos.Tool(function_declarations=[weather_function, search_function, news_function])]
    )

def get_gemini_model_basic():
    """Get basic Gemini model without tools for simple responses"""
    return genai.GenerativeModel("gemini-1.5-flash")

# --- MIDDLEWARE AND STATIC FILES ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="frontend"), name="static")
app.mount("/frontend_static", StaticFiles(directory="frontend"), name="frontend_static")

@app.get("/")
def serve_home():
    return FileResponse("../frontend/index.html")

# --- UTILITY FUNCTIONS ---
async def broadcast_murf_audio(b64: str):
    """Send a base64 audio chunk to every connected UI client."""
    to_drop = []
    for ws in list(murf_ws_clients):
        try:
            await ws.send_json({"audio": b64})
        except Exception:
            to_drop.append(ws)
    for ws in to_drop:
        murf_ws_clients.discard(ws)

def murf_tts(text):
    """Generate TTS audio using Murf or fallback."""
    try:
        headers = {
            "api-key": MURF_API_KEY,
            "Content-Type": "application/json"
        }
        payload = {
            "text": text,
            "voice_id": "en-IN-isha",
            "format": "mp3"
        }
        murf_response = requests.post(
            "https://api.murf.ai/v1/speech/generate",
            headers=headers,
            json=payload,
            timeout=15
        )
        if murf_response.status_code == 200:
            return murf_response.json().get("audioFile")
        else:
            print("Murf error:", murf_response.text)
    except Exception as e:
        print("Murf TTS exception:", e)
    return None

# --- QUOTA MANAGEMENT ---
import time
from datetime import datetime, timedelta
from collections import defaultdict

# Simple rate limiting
class RateLimit:
    def __init__(self):
        self.requests = defaultdict(list)
        self.max_requests_per_minute = 10  # Conservative limit
        
    def can_make_request(self, key: str = "default") -> bool:
        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)
        
        # Clean old requests
        self.requests[key] = [req_time for req_time in self.requests[key] if req_time > minute_ago]
        
        # Check if we can make a request
        return len(self.requests[key]) < self.max_requests_per_minute
    
    def record_request(self, key: str = "default"):
        self.requests[key].append(datetime.now())

rate_limiter = RateLimit()

def safe_gemini_call(model, prompt, stream=False, session_id="default"):
    """Safely call Gemini with rate limiting and error handling"""
    try:
        # Check rate limit
        if not rate_limiter.can_make_request(session_id):
            return "I'm processing many requests right now. Please try again in a moment."
        
        # Record the request
        rate_limiter.record_request(session_id)
        
        # Make the call
        if stream:
            return model.generate_content(prompt, stream=True)
        else:
            return model.generate_content(prompt)
            
    except Exception as e:
        error_str = str(e)
        if "quota" in error_str.lower() or "429" in error_str:
            return "I'm temporarily at capacity. Let me try a simpler response."
        elif "rate limit" in error_str.lower():
            return "Please wait a moment before asking again."
        else:
            print(f"Gemini error: {e}")
            return "I encountered an issue. Could you rephrase your question?"
# Fixed version of the stream_llm_and_tts function with proper error handling

# Add this global variable
active_murf_connections = {}
# Add this function to broadcast text responses to UI clients
# Add this function to your main.py to broadcast text to UI
async def broadcast_text_to_ui(text: str, is_final: bool = True):
    """Send text responses to UI clients for display"""
    to_drop = []
    
    message = {
        "type": "text_response",
        "text": text,
        "is_final": is_final,
        "timestamp": time.time()
    }
    
    print(f"ðŸ“¤ Broadcasting text to UI: {text[:50]}...")
    
    for ws in list(murf_ws_clients):
        try:
            await ws.send_json(message)
        except Exception as e:
            print(f"âŒ Failed to send text to UI client: {e}")
            to_drop.append(ws)
    
    # Clean up disconnected clients
    for ws in to_drop:
        murf_ws_clients.discard(ws)

# Modify your existing stream_llm_and_tts function - just add these lines
async def stream_llm_and_tts(prompt: str, session_id: str):
    """Fixed version with proper function call handling"""
    murf_ws = None
    try:
        persona = get_persona(session_id)
        voice_cfg = persona["voice"]
        system_prompt = persona["system"]

        murf_ws_url = (
            "wss://api.murf.ai/v1/speech/stream-input"
            "?format=PCM&sample_rate=16000&channel_type=MONO"
        )
        context_id = f"enhanced-{session_id}"

        async with websockets.connect(
            murf_ws_url,
            extra_headers={"api-key": MURF_API_KEY},
            ping_interval=20,
            ping_timeout=10
        ) as murf_ws:

            # Voice config
            voice_config_msg = {
                "voice_config": {
                    "voiceId": voice_cfg["voiceId"],
                    "style": voice_cfg.get("style", "Conversational"),
                    "rate": voice_cfg.get("rate", 0),
                    "pitch": voice_cfg.get("pitch", 0),
                    "sampleRate": 16000,
                    "format": "PCM",
                    "channelType": "MONO",
                    "encodeAsBase64": True
                },
                "context_id": context_id
            }
            
            await murf_ws.send(json.dumps(voice_config_msg))
            print(f"ðŸŽµ Sent voice config to Murf for session {session_id}")

            # Wait for acknowledgment before proceeding
            try:
                ack_msg = await asyncio.wait_for(murf_ws.recv(), timeout=5.0)
                ack_data = json.loads(ack_msg)
                if "error" in ack_data:
                    print(f"âŒ Murf config error: {ack_data}")
                    raise Exception("Murf configuration failed")
            except asyncio.TimeoutError:
                print("âš ï¸ No acknowledgment from Murf, proceeding anyway")

            # Check if tools are needed
            needs_tools = any(keyword in prompt.lower() for keyword in [
                'weather', 'temperature', 'forecast', 'news', 'search', 'latest', 
    'current', 'today', 'recent', 'happening', 'update', 'headlines', 
    'top', 'breaking', 'story', 'stories'
            ])
            
            if needs_tools:
                model = get_gemini_model_with_tools()
                full_prompt = f"""
{system_prompt}

You have access to these tools:
1. get_weather(city) - Get current weather for any city
2. search_web(query) - Search the web for current information  
3. get_news(topic) - Get latest news headlines

User request: "{prompt}"

IMPORTANT: If the user asks for news, headlines, latest updates, or current events, you MUST call the get_news() function. Do not provide generic responses about news - always use the tool to get real information.

For "top three headlines" or similar requests, call: get_news("latest", 3)
"""
            else:
                model = get_gemini_model_basic()
                full_prompt = f"""
{system_prompt}

User said: {prompt}

Respond naturally and concisely in your character.
"""

            response = safe_gemini_call(model, full_prompt, session_id=session_id)
            
            if isinstance(response, str):
                # Error case - send to both UI and TTS
                await broadcast_text_to_ui(response, is_final=True)
                text_msg = {
                    "text": response,
                    "context_id": context_id
                }
                await murf_ws.send(json.dumps(text_msg))
                print(f"ðŸ¤– Sent error message to TTS: {response}")
                
            elif response.candidates and len(response.candidates) > 0:
                candidate = response.candidates[0]
                
                # FIXED: Better function call detection
                if candidate.content and candidate.content.parts:
                    for part in candidate.content.parts:
                        # Check for function call more thoroughly
                        if hasattr(part, 'function_call') and part.function_call:
                            fn = part.function_call
                            tool_result = ""
                            
                            print(f"ðŸ”§ Tool called: {fn.name} with args: {dict(fn.args)}")
                            
                            # Execute the appropriate tool
                            if fn.name == "get_weather":
                                city = fn.args.get("city", "")
                                tool_result = get_weather(city)
                            elif fn.name == "search_web":
                                query = fn.args.get("query", "")
                                num_results = fn.args.get("num_results", 3)
                                tool_result = search_web(query, num_results)
                            elif fn.name == "get_news":
                                topic = fn.args.get("topic", "latest")
                                num_results = fn.args.get("num_results", 3)
                                tool_result = get_news(topic, num_results)
                            
                            print(f"ðŸ”§ Tool result (first 100 chars): {tool_result[:100]}...")
                            
                            # Generate natural response using the tool result
                            follow_up_model = get_gemini_model_basic()
                            follow_up_prompt = f"""
{system_prompt}

The user asked: "{prompt}"

Here's the information I found: {tool_result}

Please provide a natural, conversational response based on this information. 
Be concise and friendly for voice response.
"""
                            
                            follow_up_response = safe_gemini_call(follow_up_model, follow_up_prompt, stream=True, session_id=session_id)
                            
                            if isinstance(follow_up_response, str):
                                # Error in follow-up
                                await broadcast_text_to_ui(follow_up_response, is_final=True)
                                text_msg = {
                                    "text": follow_up_response,
                                    "context_id": context_id
                                }
                                await murf_ws.send(json.dumps(text_msg))
                            else:
                                # Stream the follow-up response
                                print("ðŸ¤– LLM (tool + stream): ", end="", flush=True)
                                full_text = ""
                                try:
                                    for chunk in follow_up_response:
                                        if hasattr(chunk, 'text') and chunk.text:
                                            text = chunk.text
                                            print(text, end="", flush=True)
                                            full_text += text
                                            text_msg = {
                                                "text": text,
                                                "context_id": context_id
                                            }
                                            await murf_ws.send(json.dumps(text_msg))
                                    
                                    # Send complete response to UI
                                    if full_text:
                                        await broadcast_text_to_ui(full_text, is_final=True)
                                        
                                except Exception as stream_error:
                                    print(f"Error in follow-up streaming: {stream_error}")
                                    fallback = "I found the information but had trouble formatting the response."
                                    await broadcast_text_to_ui(fallback, is_final=True)
                                    text_msg = {"text": fallback, "context_id": context_id}
                                    await murf_ws.send(json.dumps(text_msg))
                            
                            break  # Found function call, exit loop
                        
                        elif hasattr(part, 'text') and part.text:
                            # Direct text response
                            response_text = part.text
                            print(f"ðŸ¤– Direct response: {response_text}")
                            
                            await broadcast_text_to_ui(response_text, is_final=True)
                            text_msg = {
                                "text": response_text,
                                "context_id": context_id
                            }
                            await murf_ws.send(json.dumps(text_msg))
                            break
                else:
                    # Fallback if no content
                    fallback = "I couldn't process that request. Could you try rephrasing?"
                    await broadcast_text_to_ui(fallback, is_final=True)
                    text_msg = {"text": fallback, "context_id": context_id}
                    await murf_ws.send(json.dumps(text_msg))
            else:
                # No candidates
                fallback = "I didn't receive a proper response. Please try again."
                await broadcast_text_to_ui(fallback, is_final=True)
                text_msg = {"text": fallback, "context_id": context_id}
                await murf_ws.send(json.dumps(text_msg))

            print("\nâœ… LLM stream complete, waiting for Murf audio...")

            # Audio handling (keep your existing code)
            audio_received = False
            error_count = 0
            max_errors = 5
            try:
                async for msg in murf_ws:
                    try:
                        data = json.loads(msg)
                        print(f"ðŸ“¡ Received from Murf: {list(data.keys())}")
                        
                        # Handle errors more gracefully
                        if "error" in data:
                            error_count += 1
                            print(f"âŒ Murf error ({error_count}/{max_errors}): {data.get('error')}")
                            if error_count >= max_errors:
                                print("âŒ Too many Murf errors, stopping")
                                break
                            continue
                            
                        if "warning" in data:
                            print(f"âš ï¸ Murf warning: {data.get('warning')}")
                            continue
                        
                        if "audio" in data and data["audio"]:
                            audio_received = True
                            error_count = 0  # Reset error count on success
                            print(f"ðŸŽµ Broadcasting audio chunk (length: {len(data['audio'])})")
                            await broadcast_murf_audio(data["audio"])
                        
                        if data.get("final") or data.get("finalOutput"):
                            print("ðŸŽµ Final audio chunk received")
                            break
                            
                    except json.JSONDecodeError as e:
                        print(f"âš ï¸ JSON decode error from Murf: {e}")
                        continue
                        
            except Exception as audio_error:
                print(f"âš ï¸ Audio relay error: {audio_error}")
            
            if not audio_received:
                print("âš ï¸ No audio received from Murf")

    except websockets.exceptions.ConnectionClosed:
        print("âŒ Murf WebSocket connection closed")
    except Exception as e:
        print(f"âŒ Enhanced LLM->Murf streaming error: {e}")
        # Send error to UI
        error_msg = "Sorry, I encountered an error processing your request."
        await broadcast_text_to_ui(error_msg, is_final=True)

# Fixed broadcast function with better error handling
async def broadcast_murf_audio(b64: str):
    """Send a base64 audio chunk to every connected UI client - FIXED VERSION"""
    if not b64:
        print("âš ï¸ Empty audio data, skipping broadcast")
        return
        
    to_drop = []
    broadcast_count = 0
    print(f"ðŸ” Broadcasting at {datetime.now()}: {len(b64)} chars to {len(murf_ws_clients)} clients")
    print(f"ðŸ“¢ Broadcasting to {len(murf_ws_clients)} clients")
    
    for ws in list(murf_ws_clients):
        try:
            await ws.send_json({"audio": b64, "type": "audio_chunk"})
            broadcast_count += 1
        except Exception as e:
            print(f"âŒ Failed to send to client: {e}")
            to_drop.append(ws)
    
    # Clean up disconnected clients
    for ws in to_drop:
        murf_ws_clients.discard(ws)
    
    print(f"âœ… Audio broadcast to {broadcast_count} clients")

# Enhanced WebSocket endpoint for transcription
@app.websocket("/ws/stream-transcribe")
async def websocket_transcribe(ws: WebSocket):
    await ws.accept()
    session_id = ws.query_params.get("session_id", f"session_{int(time.time())}")
    print(f"ðŸŽ¤ Client connected with session_id: {session_id}")

    loop = asyncio.get_running_loop()

    client = StreamingClient(
        StreamingClientOptions(
            api_key=API_KEY,
            api_host="streaming.assemblyai.com",
        )
    )

    # Event handlers
    client.on(StreamingEvents.Begin, lambda self, e: print(f"âœ… AssemblyAI session started: {e.id}"))

    def handle_turn(self, e: TurnEvent):
        if e.end_of_turn:
            print(f"ðŸ—£ï¸ User said: '{e.transcript}'")
            # Send final transcript to UI
            asyncio.run_coroutine_threadsafe(
                ws.send_text("FINAL::" + e.transcript),
                loop
            )
            # Start LLM + TTS streaming
            print("ðŸš€ Starting LLM streaming...")
            asyncio.run_coroutine_threadsafe(
                stream_llm_and_tts(e.transcript, session_id),
                loop
            )
        else:
            # Send partial transcript
            asyncio.run_coroutine_threadsafe(
                ws.send_text("PARTIAL::" + e.transcript),
                loop
            )

    client.on(StreamingEvents.Turn, handle_turn)
    client.on(StreamingEvents.Termination, lambda self, e: print(f"ðŸ›‘ AssemblyAI session terminated"))
    client.on(
        StreamingEvents.Error,
        lambda self, err: asyncio.run_coroutine_threadsafe(
            ws.send_text("ERROR::" + str(err)),
            loop
        )
    )

    # Connect to AssemblyAI
    try:
        client.connect(StreamingParameters(sample_rate=16000))
        print("âœ… Connected to AssemblyAI")
    except Exception as e:
        print(f"âŒ Failed to connect to AssemblyAI: {e}")
        await ws.send_text("ERROR::Failed to connect to speech service")
        return

    try:
        while True:
            data = await ws.receive_bytes()
            client.stream(data)
    except Exception as e:
        print(f"âš ï¸ WebSocket closed: {e}")
    finally:
        client.disconnect(terminate=True)
        print("âœ… Disconnected from AssemblyAI")

@app.websocket("/ws/murf-audio")
async def ws_murf_audio(websocket: WebSocket):
    await websocket.accept()
    murf_ws_clients.add(websocket)
    print("ðŸŽ§ UI connected to /ws/murf-audio")
    try:
        while True:
            await asyncio.sleep(3600)
    except Exception:
        pass
    finally:
        murf_ws_clients.discard(websocket)
        print("ðŸ‘‹ UI disconnected from /ws/murf-audio")

# --- REST ENDPOINTS ---
class PersonaBody(BaseModel):
    persona_id: str

@app.post("/agent/persona/{session_id}")
def set_persona(session_id: str, body: PersonaBody):
    pid = body.persona_id
    if pid not in PERSONAS:
        return JSONResponse(status_code=400, content={"error": f"Unknown persona '{pid}'"})
    session_persona[session_id] = pid
    return {"ok": True, "session_id": session_id, "persona": pid}

@app.get("/agent/persona/{session_id}")
def get_persona_endpoint(session_id: str):
    persona = get_persona(session_id)
    return {"session_id": session_id, "active": session_persona.get(session_id, "neutral"), "persona": persona}

# --- ENHANCED CHAT ENDPOINT ---
@app.post("/agent/chat/{session_id}")
async def enhanced_chat(session_id: str, file: UploadFile):
    """Enhanced chat with weather, web search, and news capabilities"""
    try:
        # Save and transcribe audio
        filename = f"temp_{session_id}_{int(time.time())}.wav"
        with open(filename, "wb") as f:
            f.write(await file.read())

        transcriber = aai.Transcriber()
        transcript = transcriber.transcribe(filename)
        user_msg = transcript.text if transcript.text else "Could not understand audio"

        # Get persona
        persona = get_persona(session_id)
        
        # Enhanced chat with intelligent model selection
        needs_tools = any(keyword in user_msg.lower() for keyword in [
            'weather', 'temperature', 'forecast', 'news', 'search', 'latest', 
            'current', 'today', 'recent', 'happening', 'update'
        ])
        
        if needs_tools:
            model = get_gemini_model_with_tools()
            prompt = f"""
{persona['system']}

You have access to these tools:
- get_weather(city): Get current weather
- search_web(query): Search web for current info  
- get_news(topic): Get latest news

Use tools when appropriate for: "{user_msg}"
"""
        else:
            model = get_gemini_model_basic()
            prompt = f"""
{persona['system']}

User said: "{user_msg}"

Respond naturally in your character.
"""

        response = safe_gemini_call(model, prompt, session_id=session_id)
        
        # Handle response (similar to streaming logic but for REST)
        if isinstance(response, str):
            # Error case - response is already a string message
            assistant_msg = response
        elif response.candidates and response.candidates[0].content.parts:
            part = response.candidates[0].content.parts[0]
            
            if hasattr(part, "function_call") and part.function_call:
                fn = part.function_call
                tool_result = ""
                
                if fn.name == "get_weather":
                    city = fn.args.get("city", "")
                    tool_result = get_weather(city)
                elif fn.name == "search_web":
                    query = fn.args.get("query", "")
                    tool_result = search_web(query)
                elif fn.name == "get_news":
                    topic = fn.args.get("topic", "latest")
                    tool_result = get_news(topic)
                
                # Generate natural response with basic model
                basic_model = get_gemini_model_basic()
                follow_up = safe_gemini_call(basic_model, f"""
{persona['system']}

Based on this information: {tool_result}

Answer the user's question naturally: "{user_msg}"
Be concise.
""", session_id=session_id)
                
                if isinstance(follow_up, str):
                    assistant_msg = follow_up
                else:
                    assistant_msg = follow_up.text
            else:
                assistant_msg = response.text
        else:
            assistant_msg = "I'm having trouble processing that. Could you try again?"

        # Generate TTS
        audio_url = murf_tts(assistant_msg[:2900])

        # Clean up
        if os.path.exists(filename):
            os.remove(filename)

        return {
            "session_id": session_id,
            "user_message": user_msg,
            "assistant_response": assistant_msg,
            "audio_url": audio_url,
            "model": "gemini-1.5-flash-with-smart-tools"
        }

    except Exception as e:
        print(f"âŒ Enhanced chat error: {e}")
        error_msg = "I encountered an error. Please try again."
        return {
            "session_id": session_id,
            "user_message": "Error in processing",
            "assistant_response": error_msg,
            "audio_url": murf_tts(error_msg),
            "model": "error-fallback"
        }

# --- SKILL TESTING ENDPOINTS ---
@app.get("/test/weather/{city}")
def test_weather(city: str):
    """Test weather functionality"""
    result = get_weather(city)
    return {"city": city, "weather": result}

@app.get("/test/search/{query}")
def test_search(query: str):
    """Test web search functionality"""
    result = search_web(query)
    return {"query": query, "results": result}

@app.get("/test/news/{topic}")
def test_news(topic: str):
    """Test news functionality"""
    result = get_news(topic)
    return {"topic": topic, "news": result}

@app.get("/debug/search-api")
def debug_search_api():
    """Debug the search API configuration"""
    return {
        "search_api_key_set": bool(SEARCH_API_KEY),
        "search_engine_id_set": bool(SEARCH_ENGINE_ID),
        "search_api_key_length": len(SEARCH_API_KEY) if SEARCH_API_KEY else 0,
        "search_engine_id_length": len(SEARCH_ENGINE_ID) if SEARCH_ENGINE_ID else 0,
        "search_api_key_prefix": SEARCH_API_KEY[:10] + "..." if SEARCH_API_KEY else "None",
        "search_engine_id_value": SEARCH_ENGINE_ID if SEARCH_ENGINE_ID else "None"
    }

from pydantic import BaseModel

class ConfigBody(BaseModel):
    MURF_API_KEY: str = None
    ASSEMBLYAI_API_KEY: str = None
    GEMINI_API_KEY: str = None
    WEATHER_KEY: str = None
    SEARCH_API_KEY: str = None
    SEARCH_ENGINE_ID: str = None

@app.post("/config")
def update_config(config: ConfigBody):
    global MURF_API_KEY, GEMINI_API_KEY, WEATHER_KEY, SEARCH_API_KEY, SEARCH_ENGINE_ID
    global aai

    if config.MURF_API_KEY:
        MURF_API_KEY = config.MURF_API_KEY
    if config.ASSEMBLYAI_API_KEY:
        aai.settings.api_key = config.ASSEMBLYAI_API_KEY
    if config.GEMINI_API_KEY:
        GEMINI_API_KEY = config.GEMINI_API_KEY
        genai.configure(api_key=GEMINI_API_KEY)
    if config.WEATHER_KEY:
        WEATHER_KEY = config.WEATHER_KEY
    if config.SEARCH_API_KEY:
        SEARCH_API_KEY = config.SEARCH_API_KEY
    if config.SEARCH_ENGINE_ID:
        SEARCH_ENGINE_ID = config.SEARCH_ENGINE_ID

    return {"status": "ok", "message": "API keys updated successfully"}


if __name__ == "__main__":
    print("🛠️ Enhanced AI Voice Agent with Weather, Web Search & News is running!")
    print("🔧 Skills available:")
    print("  - 🌦️  Weather: Ask about weather in any city")
    print("  - 🔍 Web Search: Ask about current events or facts")
    print("  - 📰 News: Ask for latest news or news on specific topics")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
