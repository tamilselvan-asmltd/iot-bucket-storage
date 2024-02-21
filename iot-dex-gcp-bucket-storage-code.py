import datetime
import json  # Import the json module here
from google.cloud import storage


# Function to upload files into a bucket
def To_Upload_Files_Into_Bucket(service_account_info, request_json, bucket_name):
    try:
        # Authenticate using the service account JSON variable
        client = storage.Client.from_service_account_info(service_account_info)

        # Get the bucket
        bucket = client.bucket(bucket_name)

        # List blobs in the specified folder
        blobs = bucket.list_blobs(prefix="")

        # Initialize counters for files and folders
        file_count = 0
        folder_count = 0

        # Iterate through blobs to count files and folders
        for blob in blobs:
            if blob.name.endswith('/'):
                folder_count += 1
            else:
                file_count += 1

        # Create folder with the current date
        current_date = datetime.datetime.now().strftime("%Y%m%d")
        folder_name = current_date + "/"

        # Get the file name from request JSON
        if 'BarCode' not in request_json:
            print("Error: No 'BarCode' provided in request JSON.")
            return "Error: No 'BarCode' provided in request JSON."

        barcode = request_json['BarCode']

        # Replace spaces with underscores(bar code string may have empty spaces)
        barcode = barcode.replace(' ', '_')

        # Construct the file name
        file_name = str(barcode + ".json")

        # Create file content with all request JSON values
        file_content = json.dumps(request_json)

        # Write data to the file in the bucket
        blob = bucket.blob(folder_name + file_name)
        blob.upload_from_string(file_content)

        return f"Data written to bucket successfully! Bucket: {bucket_name}, Folder: {folder_name}, Filename: {file_name}"


    except Exception as e:
        print(e)
        print(type(e))

        error_message = str(e)
        if "The specified bucket does not exist" in error_message:
            return "IncorrectBucketName"
        else:
            error_message = str(e)
            print("Print ERROR :", error_message)
            words = error_message.split()

            if error_message.startswith("Error:"):
                # If the error message starts with "Error:", extract the status code from the second word
                status_code_str = words[1] if len(words) > 1 else "500"  # Default status code if not present
            else:
                # Otherwise, extract the status code from the first word
                status_code_str = words[0] if len(words) > 0 else "500"  # Default status code if not present

            try:
                # Convert the status code string to an integer
                status_code = int(status_code_str)
                return str(status_code)
            except ValueError:
                # If the status code cannot be converted to an integer, return a default status code (e.g., "500")
                return "500"  # Internal Server Error as default



# Function to verify if the bucket name is correct
def To_Verify_Bucket_Name(service_account_info, bucket_name):
    try:
        # Authenticate using the service account JSON variable
        client = storage.Client.from_service_account_info(service_account_info)

        # Get the bucket
        bucket = client.bucket(bucket_name)

        # List blobs in the specified folder
        blobs = bucket.list_blobs(prefix="")

        # Initialize counters for files and folders
        file_count = 0
        folder_count = 0

        # Iterate through blobs to count files and folders
        for blob in blobs:
            if blob.name.endswith('/'):
                folder_count += 1
            else:
                file_count += 1
        return "CorrectBucketName"
    except Exception as e:
        error_message = str(e)
        if "The specified bucket does not exist" in error_message:
            return "IncorrectBucketName"
        else:
            error_message = str(e)
            print("PRINT ERROR :", error_message)
            words = error_message.split()

            if error_message.startswith("Error:"):
                # If the error message starts with "Error:", extract the status code from the second word
                status_code_str = words[1] if len(words) > 1 else "500"  # Default status code if not present
            else:
                # Otherwise, extract the status code from the first word
                status_code_str = words[0] if len(words) > 0 else "500"  # Default status code if not present

            try:
                # Convert the status code string to an integer
                status_code = int(status_code_str)
                return str(status_code)
            except ValueError:
                # If the status code cannot be converted to an integer, return a default status code (e.g., "500")
                return "500"  # Internal Server Error as default


# Function to get data from an API
def Get_Api_Data(request):
    service_account_info = {
  "type": "service_account",
  "project_id": "asm-main-9ac34ca-dcfe237c",
  "private_key_id": "be2565cc8b3cacd2cf3d15de3bef33fb3a709a9d",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCpul1z1MY1dv93\n43wmwmnj3AIOAICYX2DzKDKtODa+O8/HfWOBygfCgfK+yLkH6/i/JdRMPW/SsC/g\np+BKxC1ez5RZm/82agQtm+fWaCXaZW1hoLZktVfP12Op3wZ6JQB5nHzJk9ipN0eq\npusNdqWOfEge9CZYl/Z4Q8epKyBk+qLdL2SCJvEOKVyGMuhbC3EFd6rTGraw1mmW\nAqP2kHi3saJLuy0D8D0dTWyF27kwPoAOFrUpnL0J0SFf9Z/YtF45fX15pYrNzY7/\nBOMY9Zvwv4cFakdePzbxWBZTaOGE0LXhGnEeDUt0oeiTtdI7KAL2Mh70fWMMc7/i\nXwjTaxt1AgMBAAECggEAAqs2Bfi+ByAwYyu2C4v6C0Wt2JeEbiDtztT+ev/LyJD7\nMulTU7qgBZxyezJ/ylKZDjRjkrFSnU786bzqB3LvPbLo48HSsQHYv/jYG+R2V+lJ\nX4Qd6hZ544BBBm5F1UzYpZDi226SNRLz8wh94K/RaZylq6ruIW1+SCTUsmHFuCC8\nOZ5TzbTE8kYE08GB/eEWw8+JyIaB1869NWng4qI0w8WBvwatRW+ZFm1TyLe+qw9f\n22HL8yi0ugeHBh/qV1ANcY7jZ/IAMRKEQXduhrIWOQwhx+Cmocrjd30soRoled6O\nQTTKIGGZEKqLdhvT3jOlBjtodXzukuKXaCpcgwmykQKBgQDSY9uOh3Q7f3BgLuvm\n5Dpj6z888eqbMi3Iw92ymiGB3kmfvMYzuYjXpbsqTL5OyfU2uNL/s0T4M0t+AplM\nOJ0TYXk7bSQKOdzCfctZqoeQYZbISij7XssAJNuvS1vPZ3K00AQwRoSujy1F7lDZ\nTWPndpM7CrmAeAvvME1hHObvvQKBgQDOhdwdhE5uvwyt5Sr/IdX73KkQ1FJsJ3Ea\nA29FcwRqfumutRvnAOc0Rehot63m6HMKyntlur6k+zm7TLs7mIpfb3jmy3EyTnRh\nr+G4maCel6RBBuq4E3JpIoeNWBlZKOk26NTV2nV/ZH8pYKI25y4A8qhIbRV6Cf4p\nsQI1dp6aGQKBgQC5M9RDfHPSKFXpJYTeRMwtxxls8dw+8AYjtF/FaC7U3rPa3Ndx\nEwMmKis/ij0AqIGmwT5kfw/YmXb8HxeJZFDJjcWljWbrZZIy13+ZmCcr7uxHthir\nZv+2SbvlbKYORpdQGcmGIZ2M+Fa3N5mOILUwPU5P7BntwD9pOBD1yvZz1QKBgQCZ\ntUVLbrKVeHouXvcuZneF+giW7v/dYUmri8zgSqeJv6WbE06OEr0BydodPkAfMPiW\nmndXErNbxWOFhKmBxJmQSD4awGYihAPYxPfh1e2Wu9RTVZuSn16y4zEyQNS06F8T\nMB1ggTScOSbvm+1/oXY63F/lb9LhPti9HPJA6mVG2QKBgQCWF3rLOO3f7NHUMG0G\np818zYnSRJCURnvciS9cMVQCBslS/s10VKJVdObGQTs2AGG+a4TPpaWK/x4/Uoob\ndHHjhHGe24KuBTpxd/B5IzSEombnkYHoRplwuO7WTUPcM2RAFc1ZZCyVE64V/oIv\n3u03FBYgR+1pQSoYRJdi6hdOmQ==\n-----END PRIVATE KEY-----\n",
  "client_email": "dex-vendor-asm-data-writer@asm-main-9ac34ca-dcfe237c.iam.gserviceaccount.com",
  "client_id": "115318807008703438008",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dex-vendor-asm-data-writer%40asm-main-9ac34ca-dcfe237c.iam.gserviceaccount.com",
  "universe_domain": "http://googleapis.com"
}
    # Check if the request method is POST
    if request.method == 'POST':
        # Get the JSON payload from the request
        request_json = request.get_json()
        # Check if the request contains a JSON payload
        if request_json:
            # Check if the JSON payload contains 'BucketName'
            if 'BucketName' in request_json:
                # Extract the bucket_name from the JSON payload
                bucket_name = request_json['BucketName']
                # Check if the bucket name is empty
                if not bucket_name:
                    # If bucket name is empty
                    return 'EmptyBucketName'
                return To_Verify_Bucket_Name(service_account_info, bucket_name)
            # Check if the JSON payload contains 'Bucket_Name' and 'BarCode' with BarCode not empty
            elif 'Bucket_Name' in request_json and 'BarCode' in request_json and request_json['BarCode']:
                # Extract the bucket_name from the JSON payload
                bucket_name = request_json['Bucket_Name']

                # Check if the bucket name is empty
                if not bucket_name:
                    return 'EmptyBucketName'
                # Call To_Upload_Files_Into_Bucket function
                return To_Upload_Files_Into_Bucket(service_account_info, request_json, bucket_name)
            else:
                # If 'Bucket_Name'  is missing
                return 'EmptyBucketName'
        else:
            # If the request contains no JSON payload,
            return "No JSON payload received"
    else:
        # If method is not POST, abort with 405 Method Not Allowed
        return 'Method not allowed. Please use POST method.'
    # If everything is successful, return 200 OK
    return "Data uploaded into: {}".format(bucket_name), 200
