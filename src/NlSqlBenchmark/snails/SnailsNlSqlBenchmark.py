from NlSqlBenchmark.NlSqlBenchmark import NlSqlBenchmark
import platform
from os.path import dirname, abspath
import docker


class SnailsNlSqlBenchmark(NlSqlBenchmark):

    name = "SNAILS"
    
    def __init__(
            self, 
            db_host_profile="docker"
            ):
        super().__init__()
        self.benchmark_folder = dirname(dirname(dirname(dirname(abspath(__file__))))) + "/benchmarks/snails"
        if db_host_profile == "docker":

            try:
                client = docker.from_env()
                client.images.get('snails-db')
            except docker.errors.ImageNotFound:
                print("The 'snails-db' image is not available locally. Please run the install script in the benchmark folder before running the benchmark.")
                raise

            try:
                print("Loading Docker container 'skaplel-running-snails'")
                container = client.containers.get("skalpel-running-snails")
                container.start()
            except docker.errors.NotFound:
                print("Container not found, running container for the first time on port 1433.")
                client.containers.run("snails-db", ports={1433:1433}, name="skalpel-running-snails", detach=True)
            self.container = container
            db_info_file = self.benchmark_folder + "/ms_sql/dbinfo.json"

        self.name = "SNAILS"
        self.naturalness = "Native"
        self.syntax = "tsql"
        pass


    def __del__(self):
        print("SnailsNlSqlBenchmark is stopping Docker container 'skalpel-running-snails'")
        self.container.kill()


    def __iter__(self):
        return self
    


    def __next__(self):
        question = self.get_active_question()
        self.active_question_no += 1
        return question
    


    def __len__(self):
        return 0
    


    def set_naturalness(self, naturalness: str) -> None:
        """
        Sets the naturalness level for the benchmark.
        Parameters:
        naturalness (str): The naturalness level to set. Must be one of ["Regular", "Low", "Least", "Native"].
        Raises:
        ValueError: If the provided naturalness level is not one of the allowed values.
        """
        if naturalness not in ["Regular", "Low", "Least", "Native"]:
            raise ValueError
        self.naturalness = naturalness



    def set_syntax(self, syntax: str) -> None:
        """
        Set the syntax type for the benchmark.
        Parameters:
        syntax (str): The syntax type to set. Must be either "tsql" or "sqlite".
        Raises:
        ValueError: If the provided syntax is not "tsql" or "sqlite".
        """
        if syntax not in ["tsql", "sqlite"]:
            raise ValueError
        self.syntax = syntax
    


    def get_active_question(self) -> dict:
        return super().get_active_question()
    


    def get_active_schema(self, database: str = None) -> dict:
        return super().get_active_schema(database)