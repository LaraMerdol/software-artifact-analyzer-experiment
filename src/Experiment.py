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
        self.experimentDateTime = datetime.datetime.now().date()
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
            filename=("logs/log-{date:%Y-%m-%d}" + ".txt").format(
                date=self.experimentDateTime
            ),
            force=True,
        )

    def connectToDatabase(self):
        uri = "bolt://ivis.cs.bilkent.edu.tr:3006"
        user = "neo4j"
        password = "01234567"
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.session = self.driver.session()
        self.session.write_transaction(self.experiment)

    @staticmethod
    def experiment(tx):
        prs = tx.run("MATCH (n:PullRequest) RETURN n.name")
        for pr in prs:
            pr = pr[0]
            startTime = time.time()
            files = tx.run(
                """
                    MATCH (N:PullRequest{name:$pr})-[:INCLUDES]-(c:Commit)-[:CONTAINS]-(f:File)
                    WITH collect(distinct elementId(f)) AS fileIds  RETURN fileIds
                    """,
                pr=pr,
            )
            changeSet = [record["fileIds"] for record in files][0]
            if len(changeSet) != 0:
                ignoreDevelopers = tx.run(
                    """
                        MATCH (N:PullRequest{name:$pr})-[:INCLUDES]-(c:Commit)-[:COMMITTED]-(d:Developer) 
                        WITH collect(distinct elementId(d)) AS ignoreDevs RETURN ignoreDevs
                        """,
                    pr=pr,
                )
                ignoreDevelopersIds = [
                    record["ignoreDevs"] for record in ignoreDevelopers
                ][0]
                accessibleDevelopers = tx.run(
                    """
                        MATCH (file:File) WHERE elementId(file) in $list
                        CALL apoc.path.subgraphAll(file, { relationshipFilter: null,  minLevel: 0,   maxLevel: 3, 
                        bfs: true })  YIELD nodes, relationships RETURN  [node IN nodes  WHERE 'Developer' IN labels(node) | elementId(node)] AS NodeIDs
                        """,
                    list=changeSet,
                )
                accessibleDevelopersIds = [
                    record["NodeIDs"] for record in accessibleDevelopers
                ][0]

                reviewers = tx.run(
                    """
                        CALL findNodesWithMostPathBetweenTable($changeSet, ['COMMENTED'], $accessibleDevelopers ,'recency',3,3, false,
                        225, 1, null, false, 'score', 0, {Commit:['createdAt','end'],Developer:['start','end'],File:['createdAt','end'],Issue:['createdAt','closeDate'],PullRequest:['createdAt','closeDate'],ASSIGNED_TO:['createdAt','end'],RESOLVED:['createdAt','end'],ASSIGNED_BY:['createdAt','end'],RENAMED_TO:['createdAt','end'],BLOCKS:['createdAt','end'],
                        COMMITTED:['createdAt','end'],COMMENTED:['createdAt','end'],INCLUDES:['createdAt','end'],DEPENDS_UPON:['createdAt','end'],DUPLICATES:['createdAt','end'],FIXES:['createdAt','end'],INCORPORATES:['createdAt','end'],MERGED:['createdAt','end'],OPENED:['createdAt','end'],IS_A_CLONE_OF:['createdAt','end'],RELATES_TO:['createdAt','end'],REFERENCED:
                        ['createdAt','end'],REVIEWED:['createdAt','end'],CLOSED:['createdAt','end'],REPORTED:['createdAt','end'],SUPERSEDES:['createdAt','end']}, -5364669352000, 4102434000000, 0, 10000, null) YIELD elementId, name, score RETURN name
                        """,
                    changeSet=changeSet,
                    accessibleDevelopers= [id for id in accessibleDevelopersIds if id not in ignoreDevelopersIds],
                )
                #reviewerNames = [record["name"] for record in reviewers][0]
                endTime = time.time()
                elapsedTime = endTime - startTime
                resultFilePath = "results/result-{date:%Y-%m-%d}.txt".format(
                    date=datetime.datetime.now().date()
                )
                f = open(resultFilePath, "a")
                f.write(f"Pull request key: {pr}, Change set Size: {len(changeSet)}, Accessible Developer: {len([id for id in accessibleDevelopersIds if id not in ignoreDevelopersIds])}, Recommendation time: {elapsedTime}  \n")
                f.close()
                # Write other c

if __name__ == "__main__":
        with open("config.yaml", "r") as stream:
            config = None
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exception:
                print(exception)

        experiment = Experiment(config)
        experiment.run()
