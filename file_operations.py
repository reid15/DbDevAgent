# Tools for reading and writing to files

import os
import shutil

def save_file(directory_path:str, filename: str, content: str):
    # Make sure the directory exists
    os.makedirs(directory_path, exist_ok=True)

    file_path = os.path.join(directory_path, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    
def list_files(directory: str) -> list[dict]:
    """
    Recursively lists all files under a given directory.
    
    Args:
        directory: Path to the directory to scan
        
    Returns:
        List of dicts with 'filepath' and 'relative_path' for each file
    """
    files = []
    directory = os.path.abspath(directory)
    
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            filepath = os.path.join(root, filename)
            files.append({
                "filepath": filepath,
                "relative_path": os.path.relpath(filepath, directory)
            })
    
    return sorted(files, key=lambda x: x["relative_path"])

def read_file(filepath: str) -> dict:
    """
    Reads the content of a file.
    
    Args:
        filepath: Absolute or relative path to the file
        
    Returns:
        Dict with 'filepath', 'content', and 'size_bytes'
    """
    filepath = os.path.abspath(filepath)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    if not os.path.isfile(filepath):
        raise ValueError(f"Path is not a file: {filepath}")
    
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    
    return {
        "filepath": filepath,
        "content": content,
        "size_bytes": os.path.getsize(filepath)
    }
