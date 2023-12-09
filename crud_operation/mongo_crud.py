from pymongo import MongoClient


class MongoDB:
    def __init__(self):
        self.client = self.get_client()
        self.database = None
        self.table = None

    def get_client(self):
        """
        Create connection to MongoDB server.
        :return:
        """
        mongo_db_server_url = "mongodb://localhost:27017"
        client = MongoClient(mongo_db_server_url)
        return client

    def create_database(self, database_name):
        """
        Create database
        :param database_name: name of the database
        :return: database obj
        """
        database = self.client[database_name]
        self.database = database
        return self.database

    def create_table(self, table_name):
        """
        Create database table
        :param table_name:
        :return: Table obj
        """
        table = self.database[table_name]
        self.table = table
        return self.table


class EmployeeService(MongoDB):
    """
    This service is responsible for all types of operation in Employee table. Specially CRUD.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()

    def create_employee(self, employee_data, in_bulk=False):
        """
        Create employee, If bulk=True, then employee will be created in bulk amount, else single.
        :param employee_data:
        :param in_bulk:
        :return:
        """
        if in_bulk:
            self.table.insert_many(employee_data)
        else:
            self.table.insert_one(employee_data)

    def retrieve_employee(self, employee_query, retrieve_multiple=False):
        """
        Employee Retrieve
        :param employee_query: Employee query, which to be retrieved
        :param retrieve_multiple: Single or Multiple retrieve
        :return:
        """
        if retrieve_multiple:
            employee = self.table.find(employee_query)
        else:
            employee = self.table.find_one(employee_query)
        return employee

    def update_employee(self, employee_query, new_data):
        """
        :param employee_query: The employee object, which will be updated
        :param new_data: New data
        :return:
        """
        present_data = self.retrieve_employee(employee_query=employee_query)

        new_data = {
            "$set": {**new_data}
        }
        employee = self.table.update_one(present_data, new_data)
        # employee = self.table.update_many(present_data, new_data)  # To update many
        return employee

    def delete_employee(self, employee_query):
        """
        Delete an employee
        :param employee_query:
        :return:
        """
        self.table.delete_one(employee_query)
        # self.table.delete_many(employee_query)


if __name__ == "__main__":
    mongo = EmployeeService()
    mongo.create_database(database_name="employee")
    mongo.create_table(table_name="employee_table")

    single_employee_info = {
        "Name": "Sharif",
        "Employee_ID": "EMP_100001",
        "Branch": "Development"
    }
    multiple_employee_info = [
        {
            "Name": "Ehan",
            "Employee_ID": "EMP_100002",
            "Branch": "Development"
        },
        {
            "Name": "Sohana",
            "Employee_ID": "EMP_100003",
            "Branch": "Management"
        }
    ]
    # Create a single employee object
    # mongo.create_employee(employee_data=single_employee_info)
    # Create employees in bulk
    # mongo.create_employee(employee_data=multiple_employee_info, in_bulk=True)

    # Retrieve Employee
    query = {
        "Name": "Abdullah Ehan"
    }
    # employee = mongo.retrieve_employee(employee_query=query, retrieve_multiple=True)
    # for i in employee:
    #     print(i)

    # employee_update_data = {
    #     "Name": "Abdullah Ehan",
    #     "Branch": "Production"
    # }
    # mongo.update_employee(employee_query=query, new_data=employee_update_data)

    # Delete Employee
    mongo.delete_employee(employee_query=query)