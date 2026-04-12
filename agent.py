# Entry point for agent - Manages tools and calls to AI API

import anthropic
import datetime
import json
import logging
import os
import yaml

from db_sql_server import get_databases, get_db_objects, get_object_definition
from dotenv import load_dotenv
from file_operations import save_file, list_files, read_file
from openai import OpenAI

print("Starting...")

# Error Logging
logging.basicConfig(
    filename="agent.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load settings from .env file
load_dotenv()

# Choose provider via env variable: "openai" (default) or "anthropic"
API_PROVIDER = os.getenv("API_PROVIDER", "openai").lower()

ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5").lower()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4-mini").lower()

openai_client = OpenAI()
anthropic_client = anthropic.Anthropic()

# Read system prompt from agent.md file
def load_system_prompt(filename="config/agent.md"):
    with open(filename, "r") as f:
        return f.read()

system_prompt = load_system_prompt()

# Map function names to actual Python functions
tool_functions = {
    "get_databases": get_databases,
    "get_db_objects": get_db_objects,
    "get_object_definition": get_object_definition,
    "save_file": save_file,
    "list_files": list_files,
    "read_file": read_file
}

# Load tool definitions from tools.yaml
def load_tools(filename="config/tools.yaml"):
    with open(filename, "r") as f:
        yaml_tools = yaml.safe_load(f)

    openai_tools = []
    anthropic_tools = []

    for t in yaml_tools:
        params = t.get("parameters", {})

        # OpenAI format (unchanged from your original)
        openai_tools.append({
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t["description"],
                "parameters": {
                    "type": "object",
                    "properties": params,
                    "required": list(params.keys())
                }
            }
        })

        # Anthropic format (slightly different shape)
        anthropic_tools.append({
            "name": t["name"],
            "description": t["description"],
            "input_schema": {
                "type": "object",
                "properties": params,
                "required": list(params.keys())
            }
        })

    return openai_tools, anthropic_tools

def call_tool(function_name, arguments):
    if function_name in tool_functions:
        func = tool_functions[function_name]
        return func(**arguments) if arguments else func()
    return {"error": f"Unknown function {function_name}"}

openai_tools, anthropic_tools = load_tools()

# Conversation memory
messages = [{"role": "system", "content": system_prompt}]

def clear_context():
    """Clear the screen and reset conversation memory."""
    os.system("cls" if os.name == "nt" else "clear")
    messages.clear()
    messages.append({"role": "system", "content": system_prompt})
    print("Context cleared.")

def export_context():
    """Save the conversation history to a text file."""
    exportable = [m for m in messages if m["role"] != "system"]
    if not exportable:
        print("Nothing to export.")
        return

    os.makedirs("file_output", exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"file_output/export_{timestamp}.txt"

    lines = [f"Export - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", "=" * 60 + "\n"]
    for m in exportable:
        role = m["role"].upper()
        content = m["content"]
        # Anthropic stores assistant content as a list of blocks — flatten to text
        if isinstance(content, list):
            content = " ".join(
                block.text for block in content if hasattr(block, "text")
            )
        lines.append(f"\n[{role}]\n{content}\n")

    with open(filename, "w", encoding="utf-8") as f:
        f.writelines(lines)

    print(f"Exported to {filename}")

    with open(filename, "w") as f:
        f.writelines(lines)
    print(f"Conversation exported to {filename}")
    
# --- OpenAI call ---
def call_openai():
    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
        tools=openai_tools,
        tool_choice="auto"
    )
    message = response.choices[0].message

    if message.tool_calls:
        messages.append(message)
        for tool_call in message.tool_calls:
            function_name = tool_call.function.name
            arguments = json.loads(tool_call.function.arguments)
            result = call_tool(function_name, arguments)
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": json.dumps(result)
            })

        final = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages
        )
        final_message = final.choices[0].message
        messages.append(final_message)
        return final_message.content
    else:
        messages.append(message)
        return message.content


# --- Anthropic call ---
def call_anthropic():
    # Anthropic takes the system prompt separately, not in the messages list
    anthropic_messages = [m for m in messages if m["role"] != "system"]

    response = anthropic_client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=8096,
        system=system_prompt,
        tools=anthropic_tools,
        messages=anthropic_messages
    )

    # Handle tool use in a loop (Anthropic may return multiple tool calls at once)
    while response.stop_reason == "tool_use":
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = call_tool(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result)
                })

        # Append assistant response and tool results to message history
        anthropic_messages.append({"role": "assistant", "content": response.content})
        anthropic_messages.append({"role": "user", "content": tool_results})

        response = anthropic_client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=8096,
            system=system_prompt,
            tools=anthropic_tools,
            messages=anthropic_messages
        )

    # Extract final text response
    final_text = next(
        (block.text for block in response.content if hasattr(block, "text")), ""
    )
    # Sync back to the shared messages list so conversation history stays intact
    anthropic_messages.append({"role": "assistant", "content": response.content})
    messages.clear()
    messages.append({"role": "system", "content": system_prompt})
    messages.extend(anthropic_messages)

    return final_text


def run_agent():
    print(f"Using provider: {API_PROVIDER.upper()}")
    while True:
        user_input = input("\nEnter Prompt: ")
        
        if user_input.lower() in ["exit", "quit"]:
            break
        elif user_input.lower() == "clear":
            clear_context()
            continue
        elif user_input.lower() == "export":
            export_context()
            continue
        elif not user_input:
            continue

        print("Submitted")            
        messages.append({"role": "user", "content": user_input})

        if API_PROVIDER == "anthropic":
            reply = call_anthropic()
        else:
            reply = call_openai()

        print("\nAgent:", reply)


if __name__ == "__main__":
    try:
        run_agent()
    except Exception as e:
        logger.critical(f"Unhandled exception caused agent to crash: {e}", exc_info=True)
        raise