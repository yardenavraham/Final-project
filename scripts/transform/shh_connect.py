import paramiko

hostname = "127.0.0.1"  # במקום "localhost"
port = 22022
username = "developer"
password = "developer"

local_file_path = "scripts/transform/roads_transform.py"         # הנתיב בקובץ מקומי
remote_file_path = "/home/developer/projects/lab/"

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

client.connect(hostname, port=port, username=username, password=password)

# פתיחת SFTP להעלאת הקובץ
sftp = client.open_sftp()
sftp.put(local_file_path, remote_file_path)
sftp.close()
print(f"File uploaded to {remote_file_path}")

remote_script_path = "/home/developer/projects/lab/roads_transform.py"

# הרצת הסקריפט דרך SSH
stdin, stdout, stderr = client.exec_command(f"spark-submit {remote_script_path}")

print("STDOUT:")
print(stdout.read().decode())
print("STDERR:")
print(stderr.read().decode())

client.close()



