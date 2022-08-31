#!/usr/bin/env python3
import json
from faker import Faker
import random
import hashlib
import string
import time
import randomname
import pickle

from graph_data_gen import DateGenerator
from graph_data_gen import DescriptionGenerator
from graph_data_gen import RandomDataGenerator

class EntityGenerator:
    accountId = None
    private = False
    
    date_generator = None
    description_generator = None
    random_data_gen = None
    filepath = None

    dumpfile_path = None

    entities = None

    issues = []
    contributors = []
    repositories = []
    resolvers = []
    branches = []
    commits = []
    
    assignee_edges = []
    reporter_edges = []
    closer_edges = []
    repository_edge = []
    resolver_edge = []
    pull_request_source_edge = []
    head_commit_edge = []
    parent_commit_edge = []
    commit_contributor_edge = []

    def __init__(self, accountId, is_private, filepath):
        self.accountId = accountId
        self.private = is_private

        self.date_generator = DateGenerator("2015-4-12T4:30", "2018-9-12T8:30", '%Y-%m-%dT%H:%M')
        self.date_generator.set_prop(0.02)
        self.description_generator = DescriptionGenerator()
        self.random_data_gen = RandomDataGenerator()
        self.filepath = filepath

        self.max_contributors = 30
        self.max_repositories = 10

    def is_private(self):
        return self.private

    def set_dump_path(self, path):
        self.dumpfile_path = path

    def save(self):
        self.save_json("issues", self.issues)
        self.save_json("contributors", self.contributors)
        self.save_json("gitRepositories", self.repositories)
        self.save_json("pullRequests", self.resolvers)
        self.save_json("branches", self.branches)
        self.save_json("commits", self.commits)
        self.save_json("issueAssignees", self.assignee_edges, edge=True)
        self.save_json("issueReporters", self.reporter_edges, edge=True)
        self.save_json("issueClosers", self.closer_edges, edge=True)
        self.save_json("gitRepoIssues", self.repository_edge, edge=True)
        self.save_json("issueResolvers", self.resolver_edge, edge=True)
        self.save_json("pullRequestSources", self.pull_request_source_edge, edge=True)
        self.save_json("branchHeadCommits", self.head_commit_edge, edge=True)
        self.save_json("commitParents", self.parent_commit_edge, edge=True)
        self.save_json("contributorCommits", self.commit_contributor_edge, edge=True)

    def convert_to_json(self, dumpfile_name, edge=False):
        full_path = ""
        if edge is True:
            full_path = self.filepath + "/edge/"+ dumpfile_name + ".json"
        else:
            full_path = self.filepath + "/doc/" + dumpfile_name + ".json"

        full_dump = self.dumpfile_path + dumpfile_name + ".dump"
        data = self.load_dump(full_dump)

        with open(full_path, 'w') as jsonfile:
            json.dump(data, jsonfile)
        
        data.clear()

    def conversion(self):
        self.convert_to_json("issues")
        self.convert_to_json("contributors")
        self.convert_to_json("gitRepositories")
        self.convert_to_json("pullRequests")
        self.convert_to_json("branches")
        self.convert_to_json("commits")
        self.convert_to_json("issueAssignees", edge=True)
        self.convert_to_json("issueReporters", edge=True)
        self.convert_to_json("issueClosers", edge=True)
        self.convert_to_json("gitRepoIssues", edge=True)
        self.convert_to_json("issueResolvers", edge=True)
        self.convert_to_json("pullRequestSources", edge=True)
        self.convert_to_json("branchHeadCommits", edge=True)
        self.convert_to_json("commitParents", edge=True)
        self.convert_to_json("contributorCommits", edge=True)

    def save_caches(self):
        self.save_dump("issues", self.issues)
        self.save_dump("contributors", self.contributors, clear_after_save=False)
        self.save_dump("gitRepositories", self.repositories, clear_after_save=False)
        self.save_dump("pullRequests", self.resolvers)
        self.save_dump("branches", self.branches)
        self.save_dump("commits", self.commits)
        self.save_dump("issueAssignees", self.assignee_edges)
        self.save_dump("issueReporters", self.reporter_edges)
        self.save_dump("issueClosers", self.closer_edges)
        self.save_dump("gitRepoIssues", self.repository_edge)
        self.save_dump("issueResolvers", self.resolver_edge)
        self.save_dump("pullRequestSources", self.pull_request_source_edge)
        self.save_dump("branchHeadCommits", self.head_commit_edge)
        self.save_dump("commitParents", self.parent_commit_edge)
        self.save_dump("contributorCommits", self.commit_contributor_edge)

    def show_info(self):
        print("Issue count: ", len(self.issues))
        print("Commit count: ", len(self.commits))
        print("Contributor count: ", len(self.contributors))
        print("Branches count: ", len(self.branches))
        print("Resolvers count: ", len(self.resolvers))
        print("Git Repository count: ", len(self.repositories))

    def save_dump(self, dumpfile, data, clear_after_save=True):
        full_dump = self.dumpfile_path + dumpfile + ".dump"

        dumped_data = self.load_dump(full_dump)
        new_data = dumped_data + data

        with open(full_dump, 'wb') as fp:
            pickle.dump(new_data, fp)

        if clear_after_save is True:
            data.clear()

    def load_dump(self, dumpfile):
        try:
            with open(dumpfile, 'rb') as fp:
                return pickle.load(fp)
        except Exception:
            print("File missing, created: ", dumpfile)
            return []
        
    def save_json(self, filename, data, edge=False, clear_after_save=False):
        full_path = ""
        if edge is True:
            full_path = self.filepath + "/edge/"+ filename + ".json"
        else:
            full_path = self.filepath + "/doc/" + filename + ".json"

        with open(full_path, 'w') as jsonfile:
            json.dump(data, jsonfile)

        # if clear_after_save is True:
        #     data.clear()

    def get_arango_keys(self, collection_type):
        arango_key = self.random_data_gen.generate_random_hash()
        arango_id = collection_type + "/" + arango_key
        return arango_id, arango_key

    def insert_if_not_exist(self, list_of_elements, element):
        list_of_elements.append(element) if element not in list_of_elements else list_of_elements

    def is_repository_count_reach_limit(self):
        if len(self.repositories) <= self.max_repositories:
            return False
        else:
            return True

    def create_issue_with_dependencies(self, commit_count):
        issue = self.get_issue()
        reporter = self.get_contributor()
        closer = self.get_contributor()
        assignee = self.get_contributor()

        contributors = []
        self.insert_if_not_exist(contributors, reporter)
        self.insert_if_not_exist(contributors, closer)
        self.insert_if_not_exist(contributors, assignee)

        assignee_edge = self.get_edge_issue_assignee(issue, assignee)
        reporter_edge = self.get_edge_issue_reporter(issue, reporter)
        closer_edge = self.get_edge_issue_closer(issue, closer)

        repository = self.get_repository()
        repository_edge = self.get_edge_issue_repository(repository, issue)

        resolver = self.get_pull_request()
        resolver_edge = self.get_edge_issue_resolver(issue, resolver)

        branch = self.get_branch()
        pull_request_source_edge = self.get_edge_pull_request_source(resolver, branch)

        commit = self.get_commit()
        head_commit_edge = self.get_edge_branch_head(branch, commit)
        commit_contributor_edge = self.get_edge_commit_contributor(commit, assignee)

        prev_child_commit = commit
        for child_commit in range(commit_count):
            child_commit = self.get_commit()
            parent_commit_edge = self.get_edge_parent_commit(prev_child_commit, child_commit)
            
            if self.random_data_gen.get_coin_flip() is True:
                contributor = self.get_commit_related_contributor(prev_child_commit)
                self.get_edge_commit_contributor(child_commit, contributor)
            else:
                contributor = self.get_contributor()
                self.get_edge_commit_contributor(child_commit, contributor)

            self.insert_if_not_exist(contributors, contributor)
            prev_child_commit = child_commit

        contributor_count = len(contributors)

        prop = self.date_generator.count_prop(commit_count, contributor_count)
        self.date_generator.set_prop(prop)

        creation_date = self.date_generator.get_random_date()
        closing_date = self.date_generator.get_random_closing_date(creation_date)

        issue = self.set_issue_dates(issue, creation_date, closing_date)
        # print(creation_date, closing_date, commit_count, contributor_count, prop)


    def get_repository(self):
        if self.is_repository_count_reach_limit() is False:
            reponame = self.random_data_gen.generate_git_repo_name()
            arango_id, arango_key = self.get_arango_keys("gitRepositories")
            repository = {
                    "accountId":self.accountId,
                    "privateElement": self.is_private(),
                    "url": "www.github/" + reponame,
                    "name": reponame,
                    "label":reponame,
                    "_key": arango_key, 
                    "_id": arango_id,
                    "_class": "com.frontendart.codee.backend.entity.domain.node.GitRepository"
                }

            self.repositories.append(repository)
            return repository
        else:
            index = random.randint(0, len(self.repositories)) - 1
            return self.repositories[index]

    def set_issue_dates(self, issue, creation_date, closing_date):
        issue["created_at"] = creation_date
        issue["closed_at"] = closing_date
        return issue

    def get_issue(self):
        arango_id, arango_key = self.get_arango_keys("issues")
        issue_id = self.random_data_gen.get_random_id()
        creation_date = self.date_generator.get_random_date()
        closing_date = self.date_generator.get_random_closing_date(creation_date)
        state = "open" if random.randint(0, 1) == 0 else "closed" 
        if state != "closed":
            closing_date = "null"

        issue = {
            "accountId":self.accountId,
            "privateElement": self.is_private(),
            "issueId": issue_id,
            "label": issue_id, 
            "body": self.description_generator.get_description(random.randint(10, 56)),
            "created_at": creation_date,
            "closed_at": closing_date,
            "status": state,
            "_key":arango_key,
            "_id":arango_id,
            "_class": "com.frontendart.codee.backend.entity.domain.node.Issue"
        }
        self.issues.append(issue)
        return issue


    def get_pull_request(self):
        arango_id, arango_key = self.get_arango_keys("pullRequests")
        pull_request_id = self.random_data_gen.get_random_id()
        url = self.random_data_gen.generate_git_repo_name("www.github.com/")
        pull_request = {
            "accountId":self.accountId,
            "privateElement": self.is_private(),
            "pullRequestId": pull_request_id,
            "url": url,
            "label": url,
            "_key":arango_key,
            "_id":arango_id,
            "_class": "com.frontendart.codee.backend.entity.domain.node.PullRequest"
        }

        self.resolvers.append(pull_request)
        return pull_request

    def get_branch(self):
        arango_id, arango_key = self.get_arango_keys("branches")
        branchName = self.random_data_gen.generate_branch_name()
        branch = {
            "accountId":self.accountId,
            "privateElement": self.is_private(),
            "branchName": branchName,
            "label": branchName,
            "_key":arango_key,
            "_id":arango_id,
            "_class": "com.frontendart.codee.backend.entity.domain.node.Branch"
        }
        self.branches.append(branch)
        return branch


    def get_commit(self):
        arango_id, arango_key = self.get_arango_keys("commits")
        hash_value = self.random_data_gen.generate_random_hash()
        opendate = self.date_generator.get_random_date()
        commit = {
                "accountId":self.accountId,
                "privateElement": self.is_private(),
                "hash": hash_value,
                "label": hash_value,
                "opened_at":opendate,
                "_key": arango_key,
                "_id": arango_id,
                "_class": "com.frontendart.codee.backend.entity.domain.node.Commit"
        }
        self.commits.append(commit)
        return commit


    def get_contributor(self):
        if len(self.contributors) < self.max_contributors:
            arango_id, arango_key = self.get_arango_keys("contributors")
            name = self.random_data_gen.get_random_username()
            contributor = {"accountId":self.accountId,
                            "privateElement": self.is_private(),
                            "name": name, 
                            "label":name,
                            "_key":arango_key, 
                            "_id":arango_id,
                            "_class": "com.frontendart.codee.backend.entity.domain.node.Contributor"
                            }
            self.contributors.append(contributor)
            return contributor
        else:
            index = random.randint(0, len(self.contributors) -1)
            # print("Using an existing contributor")
            return self.contributors[index]

    def get_edge_issue_assignee(self, issue, contributor):
        arango_id, arango_key = self.get_arango_keys("issueAssignees")
        edge = {
                "_key":arango_key, 
                "_id":arango_id,
                "_from":issue["_id"],
                "_to": contributor["_id"],
                "_class": "com.frontendart.codee.backend.entity.domain.edge.IssueAssignee"
                }
        self.assignee_edges.append(edge)
        return edge        


    def get_edge_issue_closer(self, issue, contributor):
        arango_id, arango_key = self.get_arango_keys("issueClosers")
        edge = {
                "_key":arango_key, 
                "_id":arango_id,
                "_from":issue["_id"],
                "_to": contributor["_id"],
                "_class": "com.frontendart.codee.backend.entity.domain.edge.IssueCloser"
                }
        self.closer_edges.append(edge)
        return edge        

    def get_edge_issue_reporter(self, issue, contributor):
        arango_id, arango_key = self.get_arango_keys("issueReporters")
        edge = {
                "_key":arango_key, 
                "_id":arango_id,
                "_from":issue["_id"],
                "_to": contributor["_id"],
                "_class": "com.frontendart.codee.backend.entity.domain.edge.IssueReporter"
                }
        self.reporter_edges.append(edge)
        return edge      

    def get_edge_issue_resolver(self, issue, pull_request):
        arango_id, arango_key = self.get_arango_keys("issueResolvers")
        edge = {
                "_key":arango_key, 
                "_id":arango_id,
                "_from":issue["_id"],
                "_to": pull_request["_id"],
                "_class": "com.frontendart.codee.backend.entity.domain.edge.IssueResolver"
                }
        self.resolver_edge.append(edge)
        return edge

    def get_edge_pull_request_source(self, pull_request, branch):
        arango_id, arango_key = self.get_arango_keys("pullRequestSources")
        edge = {
                "_key":arango_key, 
                "_id":arango_id,
                "_from":pull_request["_id"],
                "_to": branch["_id"],
                "_class": "com.frontendart.codee.backend.entity.domain.edge.PullRequestSource"
                }
        self.pull_request_source_edge.append(edge)
        return edge

    def get_edge_branch_head(self, branch, commit):
        arango_id, arango_key = self.get_arango_keys("branchHeadCommits")
        edge = {
                "_key":arango_key, 
                "_id":arango_id,
                "_from":branch["_id"],
                "_to": commit["_id"],
                "_class": "com.frontendart.codee.backend.entity.domain.edge.BranchHeadCommit"
                }
        self.head_commit_edge.append(edge)
        return edge

    def get_edge_parent_commit(self, parent_commit, commit):
        arango_id, arango_key = self.get_arango_keys("commitParents")
        edge = {
                "_key":arango_key, 
                "_id":arango_id,
                "_from":parent_commit["_id"],
                "_to": commit["_id"],
                "_class": "com.frontendart.codee.backend.entity.domain.edge.CommitParent"
                }
        self.parent_commit_edge.append(edge)
        return edge

    def get_edge_issue_repository(self, repository, issue):
        arango_id, arango_key = self.get_arango_keys("gitRepoIssues")
        edge = {
                "_key":arango_key, 
                "_id":arango_id,
                "_from":repository["_id"],
                "_to": issue["_id"],
                "_class": "com.frontendart.codee.backend.entity.domain.edge.GitRepoIssue"
                }
        self.repository_edge.append(edge)
        return edge

    def get_edge_commit_contributor(self, commit, contributor):
        arango_id, arango_key = self.get_arango_keys("contributorCommits")
        edge = {
                "_key":arango_key, 
                "_id":arango_id,
                "_from":commit["_id"],
                "_to": contributor["_id"],
                "_class": "com.frontendart.codee.backend.entity.domain.edge.ContributorCommit"
                }
        self.commit_contributor_edge.append(edge)
        return edge

    def get_commit_related_contributor(self, commit):
        for edge in self.commit_contributor_edge:
            if commit["_id"] == edge["_from"]:
                return edge
        return self.get_contributor()
        

if __name__ == '__main__':
    generator = EntityGenerator(1, True, "entities/")

    generator.set_dump_path("dumpfiles/")
    count = 20000
    print("Generate data: ", count)
    ratio = count /100
    
    start_time = time.time()
    for i in range(count):
        if ((i % ratio) == 0) is True:
            percent = i / ratio
            print("{}% completed".format(percent))
            print("--- %s seconds ---" % (time.time() - start_time))
            start_time = time.time()

        commit_count = random.randint(1, 25)
        generator.create_issue_with_dependencies(commit_count=commit_count)

        # generator.save_caches()

    print("Saving data.")
    # generator.conversion()
    generator.show_info()
    generator.save()

    print("Done.")