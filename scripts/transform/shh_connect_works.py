import paramiko

hostname = "127.0.0.1"  # במקום "localhost"
port = 22022
username = "developer"
password = "developer"

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

client.connect(hostname, port=port, username=username, password=password)

remote_script_path = "projects/lab/exercises_one/my_first_app.py"

stdin, stdout, stderr = client.exec_command(f"spark-submit {remote_script_path}")

print(stdout.read().decode())
print(stderr.read().decode())

client.close()
