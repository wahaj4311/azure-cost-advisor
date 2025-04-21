# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies if needed (e.g., for specific libraries)
# RUN apt-get update && apt-get install -y --no-install-recommends some-package && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# Use --no-cache-dir to reduce image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container at /app
COPY . .

# Define environment variables (e.g., for Azure credentials if not using managed identity)
# ENV AZURE_CLIENT_ID="your_client_id"
# ENV AZURE_TENANT_ID="your_tenant_id"
# ENV AZURE_CLIENT_SECRET="your_client_secret"
# ENV AZURE_SUBSCRIPTION_ID="your_subscription_id"

# Make port 80 available to the world outside this container (if needed for a web server)
# EXPOSE 80

# Define the command to run the application
# This will run when the container launches
# You can pass arguments like --html-report here or when running the container
ENTRYPOINT ["python", "cost_optimizer.py"]

# Example default command (optional, can be overridden)
# CMD ["--help"]

# Define environment variables needed by the script 
# (These should be passed during `docker run` for security, 
# but listing them here for documentation)
# ENV SMTP_HOST="smtp.example.com"
# ENV SMTP_PORT="587"
# ENV SMTP_USER="user@example.com"
# ENV SMTP_PASSWORD="your_password"
# ENV EMAIL_SENDER="sender@example.com"
# ENV EMAIL_RECIPIENT="recipient@example.com"

# Run cost_optimizer.py when the container launches
# Default command runs without cleanup or email - override as needed
CMD [ "python", "./cost_optimizer.py" ]

# Example override for cleanup and email:
# CMD [ "python", "./cost_optimizer.py", "--cleanup", "--send-email", "--wait-for-cleanup" ] 