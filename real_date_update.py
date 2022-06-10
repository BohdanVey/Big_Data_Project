import pandas as pd
import json
def first_task_precompute(df,time_start,time_end):
    df = df.groupby(['domain']).size().sort_values(ascending=False)
    dct = {
        'time_start':time_start,
        'time_end':time_end,
        'statistics':[
            {
                x:int(df[x])
            }
            for x in df.index
        ]
    }

    with open(f"data/question_1/{time_end}.json", "w") as write_file:
        json.dump(dct, write_file, indent=4)
    print("SAVED TASK 1")


def second_task_precompute(df, time_start, time_end):
    df = df[df.isBot==True]
    df = df.groupby(['domain']).size().sort_values(ascending=False)
    dct = {
        'time_start': time_start,
        'time_end': time_end,
        'statistics': [
            {
                x: int(df[x])
            }
            for x in df.index
        ]
    }
    with open(f"data/question_2/{time_end}.json", "w") as write_file:
        json.dump(dct, write_file, indent=4)
    print("SAVED TASK 2")

def third_task_precompute(df,time_start,time_end):
    df = df.groupby('userid').agg({
        'page_title': lambda x: list(x),
        'username':'first'
    })
    df = df.sort_values(by='page_title', key=lambda x: x.str.len(), ascending=False).head(20)
    dct = {
        'time_start': time_start,
        'time_end': time_end,
        'statistics':
            [{
                'userid':int(x.name),
                'size' : len(x.page_title),
                'username':x.username,
                'titles': x.page_title

            }
                for x in df.iloc
        ]
    }
    with open(f"data/question_3/{time_end}.json", "w") as write_file:
        json.dump(dct, write_file, indent=4)
    print("SAVED TASK 3")



