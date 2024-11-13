import os
import json
import threading
import time
from fastapi import FastAPI
from typing import List, Dict
import subprocess
from docker_selenium import DockerSeleniumManager, DockerContainer


MINIMUM_SELENIUM_CONTAINERS = 2


class DockerInfoCache:
    def __init__(self, update_interval=10):
        self.cache = []
        self.containers = {}
        self.assigned = {}
        self.lock = threading.Lock()
        self.update_interval = update_interval

        self.thread = None
        self.manager = DockerSeleniumManager()
        self.start_background_update()
    
    def assign_container(self) -> DockerContainer:
        with self.lock:
            unassigned = list(set(self.containers.keys()) - set(self.assigned.keys()))

            container = None
            if not unassigned:
                container = self.manager.start_selenium_docker()
                self.containers[container.name] = container
            else:
                container = self.containers[unassigned[0]]

            self.assigned[container.name] = container

            return container

    def update_docker_info(self):
        try:
            dockers = self.manager.update_docker_info()
            names = [d["Names"] for d in dockers]

            dead_containers = set(self.containers.keys()) - set(names)
            
            for name in dead_containers:
                self.containers.pop(name)
                if name in self.assigned:
                    self.assigned.pop(name)

            if (len(self.containers) - len(self.assigned)) < MINIMUM_SELENIUM_CONTAINERS:
                for _ in range(MINIMUM_SELENIUM_CONTAINERS):
                    container = self.manager.start_selenium_docker()
                    self.containers[container.name] = container
            
            print("dockers", len(dockers))
            print("containers", len(self.containers))
            print("assigned", len(self.assigned))
            
            with self.lock:
                self.cache = dockers
        except Exception as e:
            print(f"Error updating docker info: {e}")

    def start_background_update(self):
        if self.thread:
            return 

        def update_periodically():
            while True:
                try:
                    print("Updating Docker info...")
                    self.update_docker_info()
                except Exception as e:
                    print(f"Error updating Docker info: {e}")
                time.sleep(self.update_interval)

        # デーモンスレッドとして定期更新を実行
        self.thread = threading.Thread(target=update_periodically, daemon=True)
        self.thread.start()

        print("Started background Docker info update thread")

    def get_docker_info(self) -> List[Dict]:
        # スレッドセーフにキャッシュを取得
        with self.lock:
            return self.cache

# FastAPIアプリケーションの作成
app = FastAPI()

# Docker情報キャッシュの初期化
docker_info_cache = DockerInfoCache()

@app.get("/docker-info")
async def get_docker_info():
    """
    キャッシュされたDocker情報を返すエンドポイント
    """
    return docker_info_cache.get_docker_info()


@app.get("/assign")
async def assign_container():
    return json.dumps(docker_info_cache.assign_container().__dict__)

# オプション: ヘルスチェックエンドポイント
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

# サーバー起動時の追加設定（必要に応じて）
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8022)
