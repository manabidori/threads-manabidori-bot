import os
import sys
import gspread
from google.oauth2.service_account import Credentials
import json
import random
import time
import requests
from datetime import datetime, timedelta
import cloudinary
import cloudinary.uploader

class ThreadsBot:
    def __init__(self):
        """初期化"""
        print("🚀 Threads Bot 処理を開始します...")
        
        # Threads API認証情報の取得
        try:
            self.threads_app_id = os.environ['THREADS_APP_ID']
            self.threads_app_secret = os.environ['THREADS_APP_SECRET']
        except KeyError:
            print("❌ エラー: Threads API情報がSecretsに設定されていません。")
            sys.exit(1)
        
        # Cloudinary設定
        cloudinary.config(
            cloud_name=os.environ.get('CLOUDINARY_CLOUD_NAME'),
            api_key=os.environ.get('CLOUDINARY_API_KEY'),
            api_secret=os.environ.get('CLOUDINARY_API_SECRET')
        )
        
        # Google Sheets接続
        self._connect_sheets()
        
        # トークン情報の読み込み
        self._load_token_info()
        
        print("✅ 初期化完了")
    
    def _connect_sheets(self):
        """Google Sheetsに接続"""
        try:
            creds_json_str = os.environ['GOOGLE_CREDENTIALS_JSON']
            creds_dict = json.loads(creds_json_str)
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            gc = gspread.authorize(creds)
            
            spreadsheet_name = os.environ['SPREADSHEET_NAME']
            spreadsheet = gc.open(spreadsheet_name)
            
            # シート取得
            self.config_sheet = spreadsheet.worksheet('Config')
            self.posts_sheet = spreadsheet.worksheet('Posts')
            
            print("✅ Google Sheets API接続完了")
        except Exception as e:
            print(f"❌ エラー: スプレッドシート接続失敗: {e}")
            sys.exit(1)
    
    def _load_token_info(self):
        """Configシートからトークン情報を読み込む"""
        try:
            self.access_token = self.config_sheet.cell(1, 1).value
            self.user_id = self.config_sheet.cell(1, 2).value
            expires_str = self.config_sheet.cell(1, 3).value
            
            if expires_str:
                try:
                    self.expires_at = datetime.fromisoformat(expires_str.replace(' ', 'T'))
                except:
                    self.expires_at = datetime.strptime(expires_str, '%Y-%m-%d %H:%M:%S')
            else:
                self.expires_at = datetime.now() + timedelta(days=60)
            
            print(f"✅ トークン読み込み完了（有効期限: {self.expires_at.strftime('%Y-%m-%d')}）")
        except Exception as e:
            print(f"❌ エラー: トークン情報の読み込み失敗: {e}")
            sys.exit(1)
    
    def _save_token_info(self):
        """トークン情報をConfigシートに保存"""
        try:
            self.config_sheet.update_cell(1, 1, self.access_token)
            self.config_sheet.update_cell(1, 2, self.user_id)
            self.config_sheet.update_cell(1, 3, self.expires_at.isoformat())
            print("💾 トークン情報を保存しました")
        except Exception as e:
            print(f"⚠️ トークン保存エラー: {e}")
    
    def refresh_token_if_needed(self):
        """トークンの有効期限チェック＆自動リフレッシュ"""
        days_until_expiry = (self.expires_at - datetime.now()).days
        
        if days_until_expiry <= 7:
            print(f"⚠️ トークン有効期限まで残り{days_until_expiry}日")
            print("🔄 トークンをリフレッシュします...")
            
            url = "https://graph.threads.net/refresh_access_token"
            params = {
                "grant_type": "th_refresh_token",
                "access_token": self.access_token
            }
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data['access_token']
                self.expires_at = datetime.now() + timedelta(seconds=data['expires_in'])
                self._save_token_info()
                print(f"✅ トークンリフレッシュ完了！新しい有効期限: {self.expires_at.strftime('%Y-%m-%d')}")
                return True
            else:
                print(f"❌ リフレッシュ失敗: {response.text}")
                return False
        else:
            print(f"✅ トークン有効（残り{days_until_expiry}日）")
        
        return True
    
    def is_video_file(self, file_path):
        """ファイルが動画かどうかを判定"""
        video_extensions = ['.mp4', '.mov', '.avi', '.mkv', '.flv', '.wmv']
        file_ext = os.path.splitext(file_path.lower())[1]
        return file_ext in video_extensions
    
    def upload_media_to_cloudinary(self, media_path):
        """画像または動画をCloudinaryにアップロード"""
        if not os.path.exists(media_path):
            print(f"  ⚠️ ファイルが見つかりません: {media_path}")
            return None, None
        
        is_video = self.is_video_file(media_path)
        media_type = "動画" if is_video else "画像"
        
        print(f"  📤 Cloudinaryに{media_type}をアップロード中: {media_path}")
        
        try:
            upload_params = {}
            if is_video:
                upload_params['resource_type'] = 'video'
            
            result = cloudinary.uploader.upload(media_path, **upload_params)
            url = result['secure_url']
            print(f"  ✅ アップロード成功: {url}")
            return url, 'VIDEO' if is_video else 'IMAGE'
        except Exception as e:
            print(f"  ❌ アップロード失敗: {e}")
            return None, None
    
    def has_valid_media(self, media_path):
        """メディアパスが有効かどうかを厳格にチェック"""
        if media_path is None:
            return False
        if media_path == "":
            return False
        if not isinstance(media_path, str):
            media_path = str(media_path)
        media_path = media_path.strip()
        if len(media_path) == 0:
            return False
        return True
    
    def get_unposted_groups(self, records):
        """未投稿データをグループ化"""
        groups = {}
        
        for i, post in enumerate(records):
            if str(post.get('posted', '')).upper() != 'TRUE':
                post['row_index'] = i + 2
                
                thread_id_val = str(post.get('thread_id', '')).strip()
                
                if thread_id_val:
                    group_key = f"THREAD_{thread_id_val}"
                else:
                    group_key = f"SINGLE_{i}"
                
                if group_key not in groups:
                    groups[group_key] = []
                
                groups[group_key].append(post)
        
        return groups
    
    def post_to_threads(self, text, media_path=None):
        """Threadsに投稿（画像または動画）"""
        media_url = None
        media_type = None
        
        # メディアパスの厳格なチェック
        if self.has_valid_media(media_path):
            media_path = str(media_path).strip()
            
            if media_path.startswith('http'):
                # 公開URLの場合
                media_url = media_path
                # URLから動画かどうかを判定
                if self.is_video_file(media_url):
                    media_type = "VIDEO"
                    print(f"  🌐 公開動画URL使用: {media_url}")
                else:
                    media_type = "IMAGE"
                    print(f"  🌐 公開画像URL使用: {media_url}")
            else:
                # ローカルファイルの場合
                media_url, media_type = self.upload_media_to_cloudinary(media_path)
        
        url = f"https://graph.threads.net/v1.0/{self.user_id}/threads"
        params = {
            "text": text,
            "access_token": self.access_token
        }
        
        # メディアURLとmedia_typeの両方が有効な場合のみパラメータを追加
        if media_url and media_type:
            params["media_type"] = media_type
            if media_type == "VIDEO":
                params["video_url"] = media_url
                print(f"  🎬 動画付き投稿")
            else:
                params["image_url"] = media_url
                print(f"  📷 画像付き投稿")
        else:
            params["media_type"] = "TEXT"
            print(f"  📝 テキストのみ投稿")
        
        response = requests.post(url, data=params)
        
        if response.status_code != 200:
            print(f"  ❌ コンテナ作成失敗: {response.text}")
            return None
        
        container_id = response.json()['id']
        print(f"  📦 コンテナID: {container_id}")
        
        # 動画の場合は処理時間が長いので待機
        if media_type == "VIDEO":
            print("  ⏳ 動画処理中（最大30秒待機）...")
            time.sleep(30)
        elif media_url:
            print("  ⏳ 画像処理中...")
            time.sleep(5)
        
        publish_url = f"https://graph.threads.net/v1.0/{self.user_id}/threads_publish"
        publish_params = {
            "creation_id": container_id,
            "access_token": self.access_token
        }
        
        publish_response = requests.post(publish_url, data=publish_params)
        
        if publish_response.status_code == 200:
            thread_id = publish_response.json()['id']
            print(f"  ✅ 投稿成功！Thread ID: {thread_id}")
            return thread_id
        else:
            print(f"  ❌ 公開失敗: {publish_response.text}")
            return None
    
    def post_reply(self, text, reply_to_id, media_path=None):
        """リプライとして投稿（画像または動画）"""
        media_url = None
        media_type = None
        
        # メディアパスの厳格なチェック
        if self.has_valid_media(media_path):
            media_path = str(media_path).strip()
            print(f"  📎 リプライにメディアを添付: {media_path}")
            
            if media_path.startswith('http'):
                media_url = media_path
                if self.is_video_file(media_url):
                    media_type = "VIDEO"
                else:
                    media_type = "IMAGE"
            else:
                media_url, media_type = self.upload_media_to_cloudinary(media_path)
        else:
            print(f"  📝 リプライ（テキストのみ）")
        
        url = f"https://graph.threads.net/v1.0/{self.user_id}/threads"
        params = {
            "text": text,
            "reply_to_id": reply_to_id,
            "access_token": self.access_token
        }
        
        # メディアURLとmedia_typeの両方が有効な場合のみパラメータを追加
        if media_url and media_type:
            params["media_type"] = media_type
            if media_type == "VIDEO":
                params["video_url"] = media_url
            else:
                params["image_url"] = media_url
        else:
            params["media_type"] = "TEXT"
        
        response = requests.post(url, data=params)
        
        if response.status_code != 200:
            print(f"  ❌ リプライコンテナ作成失敗: {response.text}")
            return None
        
        container_id = response.json()['id']
        print(f"  📦 リプライコンテナID: {container_id}")
        
        # 動画の場合は待機時間を長く
        if media_type == "VIDEO":
            print("  ⏳ 動画処理中...")
            time.sleep(30)
        elif media_url:
            print("  ⏳ 画像処理中...")
            time.sleep(5)
        
        publish_url = f"https://graph.threads.net/v1.0/{self.user_id}/threads_publish"
        publish_params = {
            "creation_id": container_id,
            "access_token": self.access_token
        }
        
        publish_response = requests.post(publish_url, data=publish_params)
        
        if publish_response.status_code == 200:
            thread_id = publish_response.json()['id']
            print(f"    ↳ リプライ成功！Thread ID: {thread_id}")
            return thread_id
        else:
            print(f"  ❌ リプライ公開失敗: {publish_response.text}")
            return None
    
    def run(self):
        """メイン処理"""
        if not self.refresh_token_if_needed():
            print("⚠️ トークン問題により処理を中断")
            sys.exit(1)
        
        try:
            all_posts = self.posts_sheet.get_all_records()
            post_groups = self.get_unposted_groups(all_posts)
            
            if not post_groups:
                print("【通知】未投稿のツイートがありません。全てリセットして2周目に入ります。")
                row_count = len(all_posts)
                
                if row_count > 0:
                    reset_values = [['FALSE'] for _ in range(row_count)]
                    self.posts_sheet.update(
                        range_name=f'C2:C{row_count + 1}',
                        values=reset_values
                    )
                    print(">> スプレッドシートをリセットしました。")
                    
                    for post in all_posts:
                        post['posted'] = 'FALSE'
                    post_groups = self.get_unposted_groups(all_posts)
        
        except Exception as e:
            print(f"❌ エラー: データ読み込み失敗: {e}")
            sys.exit(1)
        
        if not post_groups:
            print("⚠️ 投稿可能なデータが見つかりません。")
            sys.exit(0)
        
        print(f"📊 投稿候補のグループ数: {len(post_groups)}")
        
        selected_group_key = random.choice(list(post_groups.keys()))
        selected_posts = post_groups[selected_group_key]
        
        if "THREAD_" in selected_group_key:
            selected_posts.sort(key=lambda x: int(x.get('thread_order') or 0))
            print(f"🧵 スレッド投稿を選択: {selected_group_key} ({len(selected_posts)}件)")
        else:
            print(f"📝 単発投稿を選択: 行={selected_posts[0]['row_index']}")
        
        previous_thread_id = None
        
        for idx, post in enumerate(selected_posts):
            post_text = post['text']
            media_path = post.get('image_path', '')
            row_index = post['row_index']
            
            print(f"\n{'='*50}")
            print(f"投稿 {idx+1}/{len(selected_posts)} (行: {row_index})")
            print(f"テキスト: {post_text[:50]}...")
            if self.has_valid_media(media_path):
                print(f"メディア: {media_path}")
            
            try:
                if idx == 0:
                    thread_id = self.post_to_threads(
                        post_text,
                        media_path if self.has_valid_media(media_path) else None
                    )
                    
                    if thread_id:
                        previous_thread_id = thread_id
                        self.posts_sheet.update_cell(row_index, 3, 'TRUE')
                        self.posts_sheet.update_cell(row_index, 6, thread_id)
                        print(f"  💾 シート更新完了")
                        time.sleep(3)
                    else:
                        print("  ⚠️ 親投稿失敗、このグループをスキップ")
                        break
                
                else:
                    if not previous_thread_id:
                        print("  ⚠️ 親投稿IDがないためスキップ")
                        break
                    
                    reply_id = self.post_reply(
                        post_text,
                        previous_thread_id,
                        media_path if self.has_valid_media(media_path) else None
                    )
                    
                    if reply_id:
                        self.posts_sheet.update_cell(row_index, 3, 'TRUE')
                        self.posts_sheet.update_cell(row_index, 6, reply_id)
                        print(f"  💾 シート更新完了")
                        time.sleep(3)
                    else:
                        print("  ⚠️ リプライ投稿失敗")
                        break
            
            except Exception as e:
                print(f"  ❌ エラー: 投稿処理失敗: {e}")
                break
        
        print("\n" + "="*50)
        print("🎉 Bot処理が正常に完了しました！")


def main():
    bot = ThreadsBot()
    bot.run()


if __name__ == "__main__":
    main()
