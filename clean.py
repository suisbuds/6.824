import os
import shutil

def clean_output_directory(directory):
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    print(f"Deleting file: {file_path}")
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    print(f"Deleting directory and its contents: {file_path}")
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
    else:
        print(f"The directory {directory} does not exist.")

def confirm_deletion(directory):
    if os.path.exists(directory):
        print(f"The following files and directories will be deleted from {directory}:\n")
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            print(file_path)
        response = input(f"\nAre you sure you want to delete all files in {directory}? (yes/no): ")
        return response.lower() == 'yes'
        print(f"\n")
    else:
        print(f"The directory {directory} does not exist.")
        return False

if __name__ == "__main__":
    # 使用相对路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "output")
    if confirm_deletion(output_dir):
        clean_output_directory(output_dir)
        print(f"\nAll files in {output_dir} have been deleted.")
    else:
        print("\nDeletion cancelled.")