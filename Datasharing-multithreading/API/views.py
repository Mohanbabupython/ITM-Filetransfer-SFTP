#2418 mohanbabu From app server to get request of currentdate and zipfilename and to give response of download the zip parquetfiles to the webserver..   

from rest_framework.views import APIView
from rest_framework.response import Response
from django.http import FileResponse
import os
from dotenv import load_dotenv

#from.models import RestFrameworkApikeyApikey
# from rest_framework_api_key.models import APIKey
# def get_key():
#     key,api = APIKey.objects.create_key(name="Key")
#     RestFrameworkApikeyApikey.objects.filter(prefix='ylrAfTeu').delete()
#     key1 = RestFrameworkApikeyApikey.objects.all().values()
#     print(11111111, key,api)
#     print(key1)
# get_key()

#Load environment variables
load_dotenv()
env_path =  os.getenv('env_path')
print('env_path',env_path)
config = {
    'zip_folder_write path': os.getenv('ZIP_FOLDER_WRITE_PATH')
}

class Zip_Folder_API(APIView):
    def post(self, request):
        messages = {
            'msg1': {'msg': 'Valid date in request data', 'status': 200},
            'msg2': {'msg' : 'Invalid input date or zip folder creation failed','status': 400},
        }

        try:
            payload = request.data['zip_folder_date']
            input_date = payload['date'] #eg 2024-10-27
            zip_files = [f"DS_MIS_DAILY_REP_{i}.zip" for i in range(1, 11)]
            file_index = request.data.get('file index', 0)
            
            #Validate the file Index
            if file_index < 0 or file_index >= len(zip_files):
                return Response({'msg': 'Invalid file index.','status': 400}, status=400)
            zip_file_name = zip_files[file_index]
            zip_folder_path = os.path.join(config['zip_folder_write_path'], input_date, zip_file_name)
            print (f"Looking for zip file at: (zip_folder path)")
            if os.path.exists(zip_folder_path): # Check if the zip file exists
                response = FileResponse(open(zip_folder_path,'rb'), content_type='application/zip') #Return the zip file response
                response['Content-Disposition'] = f'attachment; filename="(zip_file_name)"'
                return response
            else:
                return Response({'msg': f'Zip file (zip_file_name) not found.', 'status': 404}, status=404)
        except KeyError:
            return Response(messages['msg2'], status=400)
        except Exception as e:
            print (f"Error: (e)")
            return Response(messages ['msg2'], status=400)
