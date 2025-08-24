# File: setup_environment.py
"""
Environment setup script for PySpark Pipeline Demo
Run this script to verify your environment is properly configured
"""

import os
import sys
import subprocess
import platform

def check_python_version():
    """Check if Python version is compatible"""
    print("Checking Python version...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} - Compatible")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} - Requires Python 3.8+")
        return False

def check_java():
    """Check if Java is installed and configured"""
    print("Checking Java installation...")
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, text=True, check=True)
        java_version = result.stderr.split('\n')[0]
        print(f"✅ Java found: {java_version}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Java not found or not in PATH")
        print("Please install Java 8 or 11:")
        if platform.system() == "Linux":
            print("  sudo apt install openjdk-11-jdk")
        elif platform.system() == "Darwin":
            print("  brew install openjdk@11")
        elif platform.system() == "Windows":
            print("  Download from Oracle or use: choco install openjdk11")
        return False

def check_spark_home():
    """Check if SPARK_HOME is set"""
    print("Checking SPARK_HOME environment variable...")
    spark_home = os.environ.get('SPARK_HOME')
    if spark_home and os.path.exists(spark_home):
        print(f"✅ SPARK_HOME found: {spark_home}")
        return True
    else:
        print("❌ SPARK_HOME not set or directory doesn't exist")
        print("Please set SPARK_HOME environment variable:")
        print("  export SPARK_HOME=/path/to/spark")
        return False

def check_pyspark():
    """Check if PySpark is installed"""
    print("Checking PySpark installation...")
    try:
        import pyspark
        print(f"✅ PySpark found: version {pyspark.__version__}")
        return True
    except ImportError:
        print("❌ PySpark not found")
        print("Install with: pip install pyspark==3.4.0")
        return False

def test_spark_session():
    """Test if we can create a Spark session"""
    print("Testing Spark session creation...")
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("EnvironmentTest") \
            .config("spark.driver.memory", "512m") \
            .getOrCreate()
        
        # Create a simple DataFrame to test functionality
        data = [("test", 1), ("spark", 2)]
        df = spark.createDataFrame(data, ["name", "value"])
        count = df.count()
        
        spark.stop()
        
        print(f"✅ Spark session test passed - Created DataFrame with {count} rows")
        return True
        
    except Exception as e:
        print(f"❌ Spark session test failed: {str(e)}")
        return False

def create_directory_structure():
    """Create necessary directories"""
    print("Creating directory structure...")
    directories = [
        "./solutions",
        "./tmp",
        "./.vscode"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✅ Created directory: {directory}")

def create_requirements_file():
    """Create requirements.txt file"""
    print("Creating requirements.txt...")
    requirements = """pyspark==3.4.0
py4j==0.10.9.7
jupyter==1.0.0
findspark==2.0.1
"""
    with open("requirements.txt", "w") as f:
        f.write(requirements)
    print("✅ requirements.txt created")

def create_vscode_config():
    """Create VS Code configuration files"""
    print("Creating VS Code configuration...")
    
    # Settings
    settings_content = """{
    "python.defaultInterpreterPath": "./pyspark-env/bin/python",
    "python.terminal.activateEnvironment": true,
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "files.associations": {
        "*.py": "python"
    },
    "python.formatting.provider": "black"
}"""
    
    os.makedirs(".vscode", exist_ok=True)
    with open(".vscode/settings.json", "w") as f:
        f.write(settings_content)
    
    # Launch configuration
    launch_content = """{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Spark Pipeline Demo",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/spark_pipeline_demo.py",
            "console": "integratedTerminal",
            "env": {
                "PYTHONPATH": "${workspaceFolder}"
            }
        }
    ]
}"""
    
    with open(".vscode/launch.json", "w") as f:
        f.write(launch_content)
    
    print("✅ VS Code configuration created")

def create_docker_compose():
    """Create Docker Compose file"""
    print("Creating docker-compose.yml...")
    
    docker_compose_content = """version: '3.8'
services:
  spark-master:
    image: bitnami/spark:3.4.0
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - "8080:8080"
      - "4040:4040"
    volumes:
      - ./:/app
    working_dir: /app

  spark-worker:
    image: bitnami/spark:3.4.0
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=512m
      - SPARK_WORKER_CORES=2
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    depends_on:
      - spark-master
    volumes:
      - ./:/app
    working_dir: /app
"""
    
    with open("docker-compose.yml", "w") as f:
        f.write(docker_compose_content)
    
    print("✅ docker-compose.yml created")

def print_installation_instructions():
    """Print detailed installation instructions"""
    print("\n" + "="*60)
    print("INSTALLATION INSTRUCTIONS")
    print("="*60)
    
    system = platform.system()
    
    print("\n1. Install Java (if not already installed):")
    if system == "Linux":
        print("   sudo apt update")
        print("   sudo apt install openjdk-11-jdk")
        print("   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64")
    elif system == "Darwin":
        print("   brew install openjdk@11")
        print("   export JAVA_HOME=/opt/homebrew/opt/openjdk@11")
    elif system == "Windows":
        print("   Download from https://adoptium.net/")
        print("   Or use chocolatey: choco install openjdk11")
    
    print("\n2. Download and Setup Spark:")
    print("   wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz")
    print("   tar -xzf spark-3.4.0-bin-hadoop3.tgz")
    if system != "Windows":
        print("   sudo mv spark-3.4.0-bin-hadoop3 /opt/spark")
        print("   export SPARK_HOME=/opt/spark")
        print("   export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin")
    
    print("\n3. Create Python Virtual Environment:")
    print("   python3 -m venv pyspark-env")
    if system == "Windows":
        print("   pyspark-env\\Scripts\\activate")
    else:
        print("   source pyspark-env/bin/activate")
    
    print("\n4. Install Python Dependencies:")
    print("   pip install -r requirements.txt")
    
    print("\n5. Set Environment Variables (add to ~/.bashrc or ~/.zshrc):")
    print("   export SPARK_HOME=/opt/spark")
    print("   export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin")
    print("   export PYSPARK_PYTHON=python3")

def run_full_environment_check():
    """Run complete environment check"""
    print("="*60)
    print("PYSPARK ENVIRONMENT SETUP AND VERIFICATION")
    print("="*60)
    
    checks = [
        ("Python Version", check_python_version),
        ("Java Installation", check_java),
        ("SPARK_HOME", check_spark_home),
        ("PySpark Installation", check_pyspark),
        ("Spark Session Test", test_spark_session)
    ]
    
    results = []
    for name, check_func in checks:
        print(f"\n--- {name} ---")
        result = check_func()
        results.append((name, result))
    
    print("\n" + "="*60)
    print("ENVIRONMENT CHECK SUMMARY")
    print("="*60)
    
    all_passed = True
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{name:<20}: {status}")
        if not result:
            all_passed = False
    
    if all_passed:
        print("\n🎉 All checks passed! Your environment is ready.")
        print("You can now run: python spark_pipeline_demo.py")
    else:
        print("\n⚠️  Some checks failed. Please fix the issues above.")
        print_installation_instructions()
    
    return all_passed

def setup_project_files():
    """Create all necessary project files"""
    print("\n--- Creating Project Structure ---")
    create_directory_structure()
    create_requirements_file()
    create_vscode_config()
    create_docker_compose()
    print("✅ Project structure created successfully")

def main():
    """Main setup function"""
    print("Starting environment setup...")
    
    # Create project structure first
    setup_project_files()
    
    # Run environment checks
    env_ready = run_full_environment_check()
    
    print("\n" + "="*60)
    print("NEXT STEPS")
    print("="*60)
    
    if env_ready:
        print("1. Run the demo: python spark_pipeline_demo.py")
        print("2. Open VS Code and explore the project")
        print("3. Check Spark UI at http://localhost:4040 during execution")
        print("4. Review the solutions in the ./solutions/ directory")
    else:
        print("1. Fix the environment issues shown above")
        print("2. Re-run this script to verify fixes")
        print("3. Alternative: Use Docker setup with 'docker-compose up'")
    
    print("\nProject files created:")
    print("- spark_pipeline_demo.py (main pipeline)")
    print("- requirements.txt (Python dependencies)")
    print("- docker-compose.yml (Docker setup)")
    print("- .vscode/ (VS Code configuration)")
    print("- solutions/ (documentation)")

if __name__ == "__main__":
    main()