import paramiko
from dotenv import load_dotenv
import os

load_dotenv()

#run scripts int remote ssh
def run_remote_ssh(script_path):

    hostname = os.getenv("HOSTNAME")
    port = int(os.getenv("PORT", 22))
    username = os.getenv("DEV_USERNAME")
    password = os.getenv("PASSWORD")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print(f"📡 Connecting to {hostname}:{port} as {username}...")
    client.connect(hostname, port=port, username=username, password=password)

    print(f"🚀 Running Spark job: {script_path}")
    stdin, stdout, stderr = client.exec_command(f"spark-submit {script_path}")

    out = stdout.read().decode()
    err = stderr.read().decode()

    if out:
        print("📄 Output:\n", out)
    if err:
        print("⚠️ Errors:\n", err)

    client.close()
    print("✅ Connection closed.")

# example
#run_remote_ssh("projects/final_project/Final-project/exercises_one/my_first_app.py")
