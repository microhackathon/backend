# FastAPI Backend

## Setup & Run

1. Create and activate the virtual environment (if not already):
   ```sh
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```sh
   pip install fastapi 'uvicorn[standard]'
   ```
3. Run the server:
   ```sh
   uvicorn main:app --reload
   ```