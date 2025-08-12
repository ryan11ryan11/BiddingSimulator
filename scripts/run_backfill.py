from apps.etl.flows import e2e_collect_load

if __name__ == "__main__":
    # 최근 72시간 백필 (운영 공지/쿼터에 맞게 조정)
    print(e2e_collect_load(hours=72))