from functools import reduce
from time import sleep
from typing import Set, Union, Any, Type

import requests

JsonType = Any


def get_id(url: str) -> int:
    """
    Cuts out id from a given API url.
    Url template:
        https://pokeapi.co/api/v2/{endpoint}/{id}/
    """
    return int(url.split("/")[-2])


class Entity:
    """
    Keeps the common logic of json_data processing for all endpoints.

    Json files from API contain more data than we need. This class
    and its descendants define the logic of json_data filtering.

    identical_fields - a list of API json parts that will be added
    in the filtered json as is (no processing at all).

    fields_to_process - a dict the keys of which are the names of
    API json parts that we will somehow process before adding to the
    filtered json, values - the names of functions that will do the
    processing.

    An example:
    identical_fields = ["id", "name"]
    fields_to_process = {"pokemon": function_that_processes_this_part_of_json}

    API json:
        {"id":1,
         "name":"normal",
         "pokemon":[
          {
             "pokemon":{
                "name":"pidgey",
                "url":"https://pokeapi.co/api/v2/pokemon/16/"
             },
             "slot":1
          },
          ...],
         ...}

    Filtered json:
        {"id": 1,
         "name": "normal",
         "pokemons": [16, ...]}
    """
    def __init__(self, json_data: JsonType) -> None:
        self.identical_fields = ["id"]  # Это переменные класса!!!
        self.fields_to_process = {}
        self.raw_json_data = json_data

    def get_identical_fields(self) -> JsonType:
        """
        Returns only the fields that need no processing and their content (as is)
        """
        return {field: self.raw_json_data[field] for field in self.identical_fields}

    def process_fields(self) -> JsonType:
        """
        Returns only the fields that need processing and their processed content.
        The functions that actually do the processing are defined in the Entity
        descendants.
        """
        processed_parts_of_json_data = [function(self.raw_json_data[field_name])
                                        for field_name, function in self.fields_to_process.items()]
        return reduce(lambda a, b: {**a, **b}, processed_parts_of_json_data, {})

    def get_filtered_json(self) -> JsonType:
        """
        Unites identical_fields_data and processed_fields_data in a single
        json (dict).
        """
        identical_fields_data = self.get_identical_fields()
        processed_fields_data = self.process_fields()
        return {**identical_fields_data, **processed_fields_data}


class PokemonType(Entity):
    """
    Keeps the logic of json_data processing for the type endpoint.

    endpoint_name - the name used in API;
    name - the name used to create a json file with all the filtered data;
    priority_weight - the value used to prioritize the execution of tasks
    from Antropova_API_pool. The higher values are assigned to the smaller
    tasks in order not to wait for the most heavy tasks.
    """
    endpoint_name = "type"
    name = "types"
    priority_weight = 3

    def __init__(self, json_data: JsonType) -> None:
        super().__init__(json_data)
        self.identical_fields += ["name"]
        self.fields_to_process = {"pokemon": self.process_pokemons}

    @staticmethod
    def process_pokemons(raw_json_pokemons_data: JsonType) -> JsonType:
        """
        Takes the part of type API json that contains the data about the
        pokemons related to this type and returns a dict containing only
        pokemon ids. See the example below for details: 
        
        Input json part: 
        "pokemon":[
          {
             "pokemon":{
                "name":"pidgey",
                "url":"https://pokeapi.co/api/v2/pokemon/16/"
             },
             "slot":1
          },
          ...]
          
        Output:
        "pokemons": [16, 17, 18, ...]
        """
        return {"pokemons": [get_id(d["pokemon"]["url"]) for d in raw_json_pokemons_data]}


class Move(Entity):
    """
    Keeps the logic of json_data processing for the move endpoint.
    """
    endpoint_name = "move"
    name = "moves"
    priority_weight = 2

    def __init__(self, json_data: JsonType) -> None:
        super().__init__(json_data)
        self.identical_fields += ["name"]
        self.fields_to_process = {"learned_by_pokemon": self.process_pokemons}

    @staticmethod
    def process_pokemons(raw_json_pokemons_data: JsonType) -> JsonType:
        """
        Takes the part of move API json that contains the data about the
        pokemons related to this move and returns a dict containing only
        pokemon ids. See the example below for details:

        Input json part:
        "learned_by_pokemon":[
          {
             "name":"clefairy",
             "url":"https://pokeapi.co/api/v2/pokemon/35/"
          },
          ...]

        Output:
        "pokemons": [35, 36, ...]
        """
        return {"pokemons": [get_id(d["url"]) for d in raw_json_pokemons_data]}


class Species(Entity):
    """
    Keeps the logic of json_data processing for the species endpoint.
    """
    endpoint_name = "pokemon-species"
    name = "species"
    priority_weight = 2

    def __init__(self, json_data: JsonType) -> None:
        super().__init__(json_data)
        self.fields_to_process = {"generation": self.process_generation}

    @staticmethod
    def process_generation(raw_json_generation_data: JsonType) -> JsonType:
        """
        Takes the part of species API json that contains the data about its
        generation and returns a dict containing only the generation name.
        See the example below for details:

        Input json part:
        "generation": {"name":"generation-i",
                       "url":"https://pokeapi.co/api/v2/generation/1/"}
        Output:
        "generation": "generation-i"
        """
        return {"generation": raw_json_generation_data["name"]}


class Pokemon(Entity):
    """
    Keeps the logic of json_data processing for the Pokemon endpoint.
    """
    endpoint_name = "pokemon"
    name = "pokemons"
    priority_weight = 1

    def __init__(self, json_data: JsonType) -> None:
        super().__init__(json_data)
        self.identical_fields += ["name"]
        self.fields_to_process = {"species": self.process_species,
                                  "stats": self.process_stats}

    @staticmethod
    def process_species(raw_json_species_data: JsonType) -> JsonType:
        """
        Takes the part of pokemon API json that contains the data about its
        species and returns a dict containing only the species id.
        See the example below for details:

        Input json part:
        "species": {"name":"litleo",
                    "url":"https://pokeapi.co/api/v2/pokemon-species/667/"}
        Output:
        "species_id": 667
        """
        return {"species_id": get_id(raw_json_species_data["url"])}

    @staticmethod
    def process_stats(raw_json_stats_data: JsonType) -> JsonType:
        """
        Takes the part of pokemon API json that contains the data about its
        stats and returns a dict containing stat names and base_stat values.
        See the example below for details:

        Input json part:
        "stats":[{"base_stat":62,
                  "effort":0,
                  "stat":{"name":"hp",
                          "url":"https://pokeapi.co/api/v2/stat/1/"}},
                 ...]

        Output:
        "stats": [{"name": "hp",
                   "base": 62},
                  ...]
        """
        return {"stats": [{"name": d["stat"]["name"],
                           "base": d["base_stat"]}
                          for d in raw_json_stats_data]}


class Generation(Entity):
    """
    Currently all we need from the generation endpoint is a list
    of its ids. For that only endpoint_name, priority_weight and
    the common functionality of Entity class is enough.
    """
    endpoint_name = "generation"
    priority_weight = 3


EndpointsType = Type[Union[PokemonType, Move, Pokemon, Species, Generation]]


class APIDownloader:
    """
    Provides access to the RESTful Pokémon API.
    """
    API_URL = "https://pokeapi.co/api/v2/"
    TIMEOUT = 0.2 # Проверить значение

    def __init__(self, entity_class: EndpointsType) -> None:
        self.entity_class = entity_class
        self.endpoint_name = entity_class.endpoint_name

    @staticmethod
    def fetch_json(url: str) -> JsonType:
        """
        Downloads the json data via API (from the given url)
        """
        response = requests.get(url)
        return response.json()

    def get_ids_to_load(self) -> Set[int]:
        """
        Returns a set of all the IDs currently present in the API
        for the endpoint assigned to this instance of API_downloader
        """
        url = f"{type(self).API_URL}{self.endpoint_name}/?limit=10000" #magic number
        json_data = self.fetch_json(url)
        return {get_id(d["url"]) for d in json_data["results"]}

    def load_entity(self, id_: int) -> Entity:
        """
        Downloads the entity with a given id_ and returns an instance
        of the assigned Entity subclass: PokemonType, Move, Species or Pokemon
        """
        url = f"{type(self).API_URL}{self.endpoint_name}/{id_}/"  # вынести в init?
        json_data = self.fetch_json(url)
        sleep(type(self).TIMEOUT)
        return self.entity_class(json_data)

    def download_and_filter_all_entities(self) -> JsonType:
        """
        Returns a single filtered json data (list) for all the entities of
        the endpoint assigned to this instance of API_downloader
        """
        ids = self.get_ids_to_load()
        filtered_data = []
        for id_ in ids:
            print(id_, end=",")
            entity = self.load_entity(id_)
            filtered_data.append(entity.get_filtered_json())
        return filtered_data
