from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta
import os
from typing import List, Dict, Optional
import logging as logger
import isodate
from dotenv import load_dotenv

logger.basicConfig(
    level=logger.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
load_dotenv()


class YouTubeClient:
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize YouTube client with API key.
        Args:
            api_key (Optional[str]): YouTube API key. If None, will try to get from environment
        """
        self.api_key = api_key or os.getenv("YOUTUBE_API_KEY")

        if not self.api_key:
            raise ValueError(
                "YouTube API key not found. Please set YOUTUBE_API_KEY environment variable."
            )

        try:
            self.youtube = build("youtube", "v3", developerKey=self.api_key)
        except Exception as e:
            raise Exception(f"Failed to initialize YouTube API client: {str(e)}")

    def search_videos(
        self,
        query: str,
        max_results: int = 50,
        order: str = "relevance",
    ) -> Dict:
        try:
            search = {
                "q": query,
                "type": "video",
                "part": "id,snippet",
                "maxResults": min(max_results, 50),
                "order": order,
            }

            response = self.youtube.search().list(**search).execute()
            video_ids = [item["id"]["videoId"] for item in response["items"]]
            videos_response = self.get_video_details(video_ids)

            return {
                "videos": videos_response,
                "nextPageToken": response.get("nextPageToken"),
                "totalResults": response["pageInfo"]["totalResults"],
            }

        except HttpError as e:
            logger.error(f"An HTTP error {e.resp.status} occurred: {e.content}")
            return None

    def get_video_details(self, video_ids: List[str]) -> List[Dict]:
        try:
            videos_response = (
                self.youtube.videos()
                .list(part="snippet,contentDetails,statistics", id=",".join(video_ids))
                .execute()
            )

            videos = []
            for item in videos_response["items"]:
                video = {
                    "id": item["id"],
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "publishedAt": item["snippet"]["publishedAt"],
                    "thumbnail": item["snippet"]["thumbnails"]["high"]["url"],
                    "channelId": item["snippet"]["channelId"],
                    "channelTitle": item["snippet"]["channelTitle"],
                    "duration": str(
                        isodate.parse_duration(item["contentDetails"]["duration"])
                    ),
                    "viewCount": int(item["statistics"].get("viewCount", 0)),
                    "likeCount": int(item["statistics"].get("likeCount", 0)),
                    "commentCount": int(item["statistics"].get("commentCount", 0)),
                }
                videos.append(video)

            return videos

        except HttpError as e:
            logger.error(f"An HTTP error {e.resp.status} occurred: {e.content}")
            return None


def main():
    try:
        client = YouTubeClient()

        search_results = client.search_videos(
            query="Python programming", max_results=5, order="viewCount"
        )

        if search_results and search_results["videos"]:
            print("\nSearch Results:")
            for video in search_results["videos"]:
                print(f"\nTitle: {video['title']}")
                print(f"Views: {video['viewCount']:,}")
                print(f"URL: https://youtube.com/watch?v={video['id']}")

    except Exception as e:
        logger.error(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
