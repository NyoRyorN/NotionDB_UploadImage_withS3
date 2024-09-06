import os, glob
from notion_client import Client
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd

def upload_image_to_s3(file_path, s3_file_name):
    """
    画像をS3にアップロードする
    
    Parameters
    ----------
    file_path : str
        ローカルでの画像ファイルのパス
    s3_file_name : str
        S3にアップロードする際のファイル名
    """
    # S3に画像をアップロードための設定
    s3 = boto3.client('s3',
                      aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
                      aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"))
    
    # S3のバケット名を取得
    bucket_name = os.getenv("AWS_S3_BUCKET_NAME")

    # 画像をS3にアップロード
    try:
        # ファイルをアップロードし、そのアップロードされたファイルのURLを取得
        s3.upload_file(file_path, bucket_name, s3_file_name)
        url = f"https://{bucket_name}.s3.amazonaws.com/{s3_file_name}"
        return url
    # ファイルが見つからない場合
    except FileNotFoundError:
        print("The file was not found")
        return None
    # 認証情報がない場合
    except NoCredentialsError:
        print("Credentials not available")
        return None

def main():
    # 環境変数の読み込み
    load_dotenv()
    
    # Notion APIの設定
    token = os.getenv("NOTION_API_KEY")
    client = Client(auth=token)
    database_id = os.getenv("NOTION_DB_ID")

    # 各データに設定するアイコンを宣言
    # 今回は仮にビリヤードのキューのアイコンを設定
    emoji = "🎱"

    # データが入っているcsvファイルを読み込む
    df = pd.read_csv("pokemon.csv", header=0)

    # 各データに対して画像をS3にアップロードしてURLを取得
    for i in range(len(df)):
        s3_file_name = df["imagepath"][i]
        image_url = upload_image_to_s3(os.path.join("images", s3_file_name), s3_file_name)
        
        # 画像のアップロードに失敗した場合は処理を終了
        if image_url == "":
            print("Failed to upload image to S3")
            exit()

        # Notionにページを作成
        try:
            # ページを作成
            response = client.pages.create(
                **{
                    "parent": { "database_id": database_id },
                    "icon":{
                        "emoji": emoji
                    },
                    # ページのプロパティを指定する
                        # データベースと同じプロパティ名を指定する
                        # 異なるプロパティ名を指定するとエラーが発生する
                        # 中身がNanの場合にもエラーが発生するので注意
                    "properties": {
                        "Name": {# タイトルのプロパティ名
                            "title": [# タイトルのプロパティを宣言するときはtitleを指定する
                                {
                                    "text": {# タイトルのプロパティの中身はtext型
                                        "content": df["name"][i] 
                                    }
                                }
                            ]
                        },   
                        "Type1": {"select": {"name": df["type1"][i]}},# セレクトボックスのプロパティ名
                        # セレクトボックスのプロパティの中身はselect型でType2に関しては値がない場合があるのでNoneを設定
                        "Type2": {"select": {"name": df["type2"][i] if pd.notnull(df["type2"][i]) else "None"}},
                        "Height": {
                            "number": float(df["height"][i]),# 数値型のプロパティ名
                        },
                        "Weight": {
                            "number": float(df["weight"][i]),# 数値型のプロパティ名
                        },
                    },  
                    # ページ内にコンテンツを追加する場合はchildrenを指定する
                    "children" : [
                        # toc block　目次ブロック
                        {
                            "object": "block",
                            "type": "table_of_contents",
                            "table_of_contents" : {}
                        },
                        # heading 1 見出し1ブロック
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
                        #Image　画像ブロック
                        {
                            "object": "block",
                            "type": "image",
                            "image": {
                                "type": "external",
                                "external": {
                                    "url": image_url # 画像のURLを指定
                                    # 外部リンクの場合はexternalを指定する
                                    # 内部リンクの場合はfileを指定する
                                    # ローカルファイルのパスを指定することはできない
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