from dbsp import DBSPConnection

def main():
    dbsp = DBSPConnection()
    project = dbsp.new_project(name = "foo", sql_code = "create table bar(name string);")
    project.status()
    project.compile()

if __name__ == "__main__":
    main()
