import datetime
import json
import logging
import time
import os
from collections import Counter
import yaml
from dateutil import parser
from neo4j import GraphDatabase


class Experiment:
    def __init__(self, config):
        self.experimentDateTime = datetime.datetime.now()
        self.configureLogging()
        self.config = config
        self.results = []
        self.connectToDatabase()

        logging.info(
            f"Experiment started at {self.experimentDateTime} with configuration: {self.config}"
        )

    def configureLogging(self):
        logging.basicConfig(
            level=logging.INFO,
            filename=(
                "logs/log-{date:%Y-%m-%d_%H.%M.%S}__" + str(os.getpid()) + ".txt"
            ).format(date=self.experimentDateTime),
            force=True,
        )

    def connectToDatabase(self):
        uri = "bolt://ivis.cs.bilkent.edu.tr:3006"
        user =  "neo4j"
        password ="01234567"
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.session = self.driver.session()
        self.session.write_transaction(self.experiment)

    @staticmethod
    def recommendReviewers(tx, pr):
            changeSet = tx.changeSet( pr)
            accessibleDevelopers = tx.accessibleDevelopers(changeSet)
            ignoreDevelopers = tx.ignoreDevelopers(pr)

            query = ""
          
            try:
                result = tx.run(
                    query )
            except:
                logging.exception("")
                recommendedReviewers = [result["data"]]
                print(recommendedReviewers)

    @staticmethod
    def accessibleDevelopers(tx, files):
            try:
                return  tx.run(
                     """
                MATCH (file:File) WHERE elementId(file) in [$list]
                CALL apoc.path.subgraphAll(file, { relationshipFilter: null,  minLevel: 0,   maxLevel: 3, 
                bfs: true })  YIELD nodes, relationships RETURN  [node IN nodes  WHERE 'Developer' IN labels(node) | node] AS NodeIDs
            """,
             list = ','.join(f"'{item}'" for item in files) )
            except:
                logging.exception("")

    @staticmethod
    def chageSet(tx, pr):
            try:
                return  tx.run(
                     """
            MATCH (N:PullRequest{name:'$pr'})-[:INCLUDES]-(c:Commit)-[:CONTAINS]-(f:File)
            WITH collect(distinct elementId(f)) AS fileIds  RETURN fileIds
            """,
             pr = pr )
            except:
                logging.exception("")   
    @staticmethod
    def ignoreDevelopers(tx, pr):
            try:
                return  tx.run(
                     """
            MATCH (N:PullRequest{name:'$pr'})-[:INCLUDES]-(c:Commit)-[:COMMITTED]-(d:Developer) 
        WITH collect(distinct elementId(d)) AS ignoreDevs return ignoreDevs
            """,
             pr = pr )
            except:
                logging.exception("")


    def experiment(tx):
        prs = tx.run(
            "MATCH (n:PullRequest)\
            RETURN elementId(n)"
        )
        for pr in prs:
            startTime = time.time()
            files = tx.run(
                     """
            MATCH (N:PullRequest{name:'$pr'})-[:INCLUDES]-(c:Commit)-[:CONTAINS]-(f:File)
            WITH collect(distinct elementId(f)) AS fileIds  RETURN fileIds
            """,
             pr = pr )
            accessibleDevelopers = tx.run(
                     """
                MATCH (file:File) WHERE elementId(file) in [$list]
                CALL apoc.path.subgraphAll(file, { relationshipFilter: null,  minLevel: 0,   maxLevel: 3, 
                bfs: true })  YIELD nodes, relationships RETURN  [node IN nodes  WHERE 'Developer' IN labels(node) | node] AS NodeIDs
            """,
             list = ','.join(f"'{item}'" for item in files) )
            ignoreDevelopers =  tx.run(
                     """
            MATCH (N:PullRequest{name:'$pr'})-[:INCLUDES]-(c:Commit)-[:COMMITTED]-(d:Developer) 
        WITH collect(distinct elementId(d)) AS ignoreDevs return ignoreDevs
            """,
             pr = pr )

            endTime = time.time()
            elapsedTime = endTime - startTime
            resultFilePath = (
                "results/result-{date:%Y-%m-%d_%H.%M.%S}__" + str(os.getpid()) + ".txt"
            ).format(date=tx.experimentDateTime)
            with open(resultFilePath, "w") as resultFile:
                print(f"Recommendation time: {elapsedTime}", file=resultFile)


if __name__ ==  '__main__':
    with open("config.yaml", "r") as stream:
        config = None
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exception:
            print(exception)

    experiment = Experiment(config)
    experiment.run()