from dbsp import DBSPConnection

def main():
    dbsp = DBSPConnection()
    print("Connection established")
    project = dbsp.new_project(name = "foo", sql_code = "create table bar(name string);")
    print("Project created")
    status = project.status()
    print("Project status: " + status)
    project.compile()
    print("Project compiled")
    status = project.status()
    print("Project status: " + status)

if __name__ == "__main__":
    main()
