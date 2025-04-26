def get_input_path(path, date,table_type):
    return f"{path}/{table_type}_{date}.csv"

def should_continue(results):
    for row in results:
        if row["constraint_status"] != "Success":
            return False
    return True