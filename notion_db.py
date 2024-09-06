import os, glob
from notion_client import Client
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd

def upload_image_to_s3(file_path, s3_file_name):
    s3 = boto3.client('s3',
                      aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
                      aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"))
    bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
    try:
        s3.upload_file(file_path, bucket_name, s3_file_name)
        url = f"https://{bucket_name}.s3.amazonaws.com/{s3_file_name}"
        return url
    except FileNotFoundError:
        print("The file was not found")
        return None
    except NoCredentialsError:
        print("Credentials not available")
        return None

def main():
    load_dotenv()
    
    token = os.getenv("NOTION_API_KEY")
    client = Client(auth=token)
    database_id = os.getenv("NOTION_DB_ID")
    emoji = "🎱"

    df = pd.read_csv("pokemon.csv", header=0)
    for i in range(len(df)):
        s3_file_name = df["imagepath"][i]
        image_url = upload_image_to_s3(os.path.join("images", s3_file_name), s3_file_name)
        
        # 画像をS3アップロードしてURLを取得
        if image_url == "":
            print("Failed to upload image to S3")
            exit()
        
        print(df["name"][i], df["type1"][i], df["type2"][i], df["height"][i], df["weight"][i], image_url)

        try:
            response = client.pages.create(
                **{
                    "parent": { "database_id": database_id },
                    "icon":{
                        "emoji": emoji
                    },
                    # ページのプロパティを指定する
                    "properties": {
                        "Name": {
                            "title": [
                                {
                                    "text": {
                                        "content": df["name"][i]
                                    }
                                }
                            ]
                        },   
                        "Type1": {"select": {"name": df["type1"][i]}},
                        "Type2": {"select": {"name": df["type2"][i] if pd.notnull(df["type2"][i]) else "None"}},
                        "Height": {
                            "number": float(df["height"][i]),
                        },
                        "Weight": {
                            "number": float(df["weight"][i]),
                        },
                    },  # end properties
                    # ページ内にコンテンツを追加する場合はchildrenを指定する
                    "children" : [
                        # toc block
                        {
                            "object": "block",
                            "type": "table_of_contents",
                            "table_of_contents" : {}
                        },
                        # heading 1
                        {
                            "object": "block",
                            "type": "heading_1",
                            "heading_1" : {
                                "rich_text": [
                                    {
                                        "text": {
                                            "content": df["name"][i]
                                        }
                                    }
                                ]
                            }
                        },
                        #Image
                        {
                            "object": "block",
                            "type": "image",
                            "image": {
                                "type": "external",
                                "external": {
                                    "url": image_url
                                }
                            }
                        }
                    ]
                }
            )
            print(response)
        except Exception as e:
            print(e)

main()