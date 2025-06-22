import os
import pandas as pd
import matplotlib.pyplot as plt
from bagpy import bagreader
from pymongo import MongoClient

def process_bag(bag_path: str, suffix: str):
    print(f"\n=== Przetwarzam zestaw {suffix}: {bag_path} ===")
    # 1) Wczytanie bag i CSV
    bag = bagreader(bag_path)
    df_topics = bag.topic_table
    topic_col = next(col for col in df_topics.columns if 'topic' in col.lower())
    traj_topics = [t for t in df_topics[topic_col] if 'traj' in t.lower()]
    topic = traj_topics[0] if traj_topics else df_topics[topic_col].iloc[0]

    csv_path = bag.message_by_topic(topic)
    df_traj = pd.read_csv(csv_path)
    print(f"Wczytano {len(df_traj)} rekordów z tematu '{topic}'")

    # 2) Statystyki
    num = df_traj.select_dtypes(include=['number'])
    stats = pd.DataFrame({
        'Max':     num.max(),
        'Min':     num.min(),
        'Mean':    num.mean(),
        'Std Dev': num.std()
    }).round(3)

    pd.set_option('display.max_rows',    None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width',       1000)
    print(f"\n—— Podstawowe statystyki ({suffix}) ——")
    print(stats)

    # 3) Histogramy
    df_traj[num.columns].hist(figsize=(12, 8), bins=50)
    plt.suptitle(f"Histogramy kolumn numerycznych ({suffix})")
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.show()

    # 4) Wstawianie do MongoDB (oddzielne bazy dla D i H)
    client = MongoClient("mongodb://localhost:27017/")
    db_name = f"ros_data_{suffix}"
    db = client[db_name]
    coll_traj   = db["trajectories"]
    coll_topics = db["topics"]
    coll_traj.drop()
    coll_topics.drop()

    traj_records   = df_traj.to_dict(orient="records")
    topics_records = df_topics.to_dict(orient="records")

    try:
        res1 = coll_traj.insert_many(traj_records)
        print(f"[{suffix}] Wstawiono {len(res1.inserted_ids)} dokumentów do '{db_name}.trajectories'")
    except Exception as e:
        print(f"[{suffix}] Błąd przy wstawianiu trajektorii:", e)

    try:
        res2 = coll_topics.insert_many(topics_records)
        print(f"[{suffix}] Wstawiono {len(res2.inserted_ids)} dokumentów do '{db_name}.topics'")
    except Exception as e:
        print(f"[{suffix}] Błąd przy wstawianiu tematów:", e)

    # 5) Rysowanie trajektorii
    x_col = next(col for col in df_traj.columns if '.position.x' in col.lower())
    y_col = next(col for col in df_traj.columns if '.position.y' in col.lower())

    plt.figure()
    plt.plot(df_traj[x_col], df_traj[y_col], marker='.', linewidth=0.5)
    plt.xlabel('X [m]')
    plt.ylabel('Y [m]')
    plt.title(f'Trajektoria – rzut XY ({suffix})')
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    sets = [
        (r'C:\Users\rimpe\Desktop\Projekty_Python\HurtownieDanychProjekt\venv\Data\D_trajectories.bag', 'D'),
        (r'C:\Users\rimpe\Desktop\Projekty_Python\HurtownieDanychProjekt\venv\Data\H_trajectories.bag', 'H'),
    ]
    for path, suffix in sets:
        process_bag(path, suffix)