import os
import subprocess

def main():
    # Check if the hidden .env file already exists
    if not os.path.exists(".env"):
        print("\n🚀 Welcome to ET-Signal-Radar Setup!")
        print("-" * 40)
        api_key = input("Please enter your Groq API Key to continue: ")
        
        # Create the .env file and save the key
        with open(".env", "w") as file:
            file.write(f"GROQ_API_KEY={api_key}\n")
            
        print("✅ API Key saved securely to local .env file!\n")
    else:
        print("\n✅ .env file found. Loading API keys...\n")

    # Automatically start the FastAPI server
    print("Starting the backend server...")
    subprocess.run(["python", "-m", "uvicorn", "api:app", "--reload"])

if __name__ == "__main__":
    main()