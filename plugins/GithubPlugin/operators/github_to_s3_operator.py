from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from GithubPlugin.hooks.github_hook import GithubHook
from airflow.hooks import S3Hook
from flatten_json import flatten
import logging
import json


class GithubToS3Operator(BaseOperator):
    """
    Github To S3 Operator
    :param github_conn_id:           The Github connection id.
    :type github_conn_id:            string
    :param github_org:               The Github organization.
    :type github_org:                string
    :param github_repo:              The Github repository. Required for
                                     commits, commit_comments, issue_comments,
                                     and issues objects.
    :type github_repo:               string
    :param github_object:            The desired Github object. The currently
                                     supported values are:
                                        - commits
                                        - commit_comments
                                        - issue_comments
                                        - issues
                                        - members
                                        - organizations
                                        - pull_requests
                                        - repositories
    :type github_object:             string
    :param payload:                  The associated github parameters to
                                     pass into the object request as
                                     keyword arguments.
    :type payload:                   dict
    :param s3_conn_id:               The s3 connection id.
    :type s3_conn_id:                string
    :param s3_bucket:                The S3 bucket to be used to store
                                     the Github data.
    :type s3_bucket:                 string
    :param s3_key:                   The S3 key to be used to store
                                     the Github data.
    :type s3_key:                    string
    """

    template_field = ['s3_key', 'payload']

    @apply_defaults
    def __init__(self,
                 github_conn_id,
                 github_org,
                 github_object,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 github_repo=None,
                 payload={},
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.github_conn_id = github_conn_id
        self.github_org = github_org
        self.github_repo = github_repo
        self.github_object = github_object
        self.payload = payload
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        if self.github_object.lower() not in ('commits',
                                              'commit_comments',
                                              'issue_comments',
                                              'issues',
                                              'members',
                                              'organizations',
                                              'pull_requests',
                                              'repositories'):
            raise Exception('Specified Github object not currently supported.')

    def execute(self, context):
        g = GithubHook(self.github_conn_id)
        s3 = S3Hook(self.s3_conn_id)
        output = []

        if self.github_object not in ('members',
                                      'organizations',
                                      'repositories'):
            if self.github_repo == 'all':
                repos = [repo['name'] for repo in
                         self.paginate_data(g,
                                            self.methodMapper('repositories'))]
                for repo in repos:
                    output.extend(self.retrieve_data(g, repo=repo))
            elif isinstance(self.github_repo, list):
                repos = self.github_repo
                for repo in repos:
                    output.extend(self.retrieve_data(g, repo=repo))
            else:
                output = self.retrieve_data(g, repo=self.github_repo)
        else:
            output = self.retrieve_data(g, repo=self.github_repo)
        output = '\n'.join([json.dumps(flatten(record)) for record in output])
        s3.load_string(
            string_data=output,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )

    def retrieve_data(self, g, repo=None):
        """
        This method builds the endpoint and passes it into
        the "paginate_data" method. It is wrapped in a
        "try/except" in the event that an HTTP Error is thrown.
        This can happen when making a request to a page that
        does not yet exist (e.g. requesting commits from a
        repo that has no commits.)
        """
        try:
            endpoint = self.methodMapper(self.github_object,
                                         self.github_org,
                                         repo)
            return self.paginate_data(g, endpoint)
        except:
            logging.info('Resource is unavailable.')
            return ''

    def paginate_data(self, g, endpoint):
        """
        This method takes care of request building and pagination.
        It retrieves 100 at a time and continues to make
        subsequent requests until it retrieves less than 100 records.
        """
        output = []
        final_payload = {'per_page': 100, 'page': 1}
        for param in self.payload:
            final_payload[param] = self.payload[param]

        response = g.run(endpoint, final_payload).json()
        output.extend(response)
        logging.info('Retrieved: ' + str(final_payload['per_page'] *
                                         final_payload['page']))
        while len(response) == 100:
            final_payload['page'] += 1
            response = g.run(endpoint, final_payload).json()
            logging.info('Retrieved: ' + str(final_payload['per_page'] *
                                             final_payload['page']))
            output.extend(response)
        output = [self.filterMapper(record) for record in output]
        return output

    def methodMapper(self, github_object, org=None, repo=None):
        """
        This method maps the desired object to the relevant endpoint
        according to v3 of the Github API.
        """
        mapping = {"commits": "repos/{0}/{1}/commits".format(org, repo),
                   "commit_comments": "repos/{0}/{1}/comments".format(org, repo),
                   "issue_comments": "repos/{0}/{1}/issues/comments".format(org, repo),
                   "issues": "repos/{0}/{1}/issues".format(org, repo),
                   "members": "orgs/{0}/members".format(org),
                   "organizations": "user/organizations",
                   "pull_requests": "repos/{0}/{1}/pulls".format(org, repo),
                   "repositories": "orgs/{0}/repos".format(self.github_org)
                   }

        return mapping[github_object]

    def filterMapper(self, record):
        """
        This process strips out unnecessary objects (i.e. ones
        that are duplicated in other core objects).
        Example: a commit returns all the same user information
        for each commit as already returned the members endpoint).
        In most cases, id for these striped objects are kept for
        reference although multiple values can be added to the array.

        Labels is currently returned as an array of dicts.
        When flattened, this can cause an undo amount of
        columns with the naming convention labels_0_name,
        labels_1_name, etc. Until a better data model can be
        determined (possibly putting labels in their own table)
        these fields are striped out entirely.

        In situations where there are no desired retention fields,
        "retained" should be set to "None".
        """
        mapping = [{'name': 'commits',
                    'filtered': 'author',
                    'retained': ['id']
                    },
                   {'name': 'commits',
                    'filtered': 'committer',
                    'retained': ['id']
                    },
                   {'name': 'issues',
                    'filtered': 'milestone',
                    'retained': ['id']
                    },
                   {'name': 'issues',
                    'filtered': 'assignee',
                    'retained': ['id']
                    },
                   {'name': 'issues',
                    'filtered': 'user',
                    'retained': ['id']
                    },
                   {'name': 'issues',
                    'filtered': 'closed_by',
                    'retained': ['id']
                    },
                   {'name': 'issues',
                    'filtered': 'labels',
                    'retained': None
                    },
                   {'name': 'issue_comments',
                    'filtered': 'user',
                    'retained': ['id']
                    },
                   {'name': 'commit_comments',
                    'filtered': 'user',
                    'retained': ['id']
                    },
                   {'name': 'repositories',
                    'filtered': 'owner',
                    'retained': ['id']
                    },
                   {'name': 'pull_requests',
                    'filtered': 'assignee',
                    'retained': ['id']
                    },
                   {'name': 'pull_requests',
                    'filtered': 'milestone',
                    'retained': ['id']
                    },
                   {'name': 'pull_requests',
                    'filtered': 'head',
                    'retained': ['label']
                    },
                   {'name': 'pull_requests',
                    'filtered': 'base',
                    'retained': ['label']
                    },
                   {'name': 'pull_requests',
                    'filtered': 'user',
                    'retained': ['id']
                    }
                   ]

        def process(record, mapping):
            """
            This method processes the data according to the above mapping.
            There are a number of checks throughout as the specified filtered
            object and desired retained fields will not always exist in each
            record.
            """

            for entry in mapping:
                # Check to see if the filtered value exists in the record
                if (entry['name'] == self.github_object) and (entry['filtered'] in list(record.keys())):
                    # Check to see if any retained fields are desired.
                    # If not, delete the object.
                    if entry['retained']:
                        for retained_item in entry['retained']:
                            # Check to see the filterable object exists in the
                            # specific record. This is not always the case.
                            # Check to see the retained field exists in the
                            # filterable object.
                            if record[entry['filtered']] is not None and\
                                    retained_item in list(record[entry['filtered']].keys()):
                                    # Bring retained field to top level of
                                    # object with snakecasing.
                                    record["{0}_{1}".format(entry['filtered'],
                                                            retained_item)] = \
                                        record[entry['filtered']][retained_item]
                    if record[entry['filtered']] is not None:
                        del record[entry['filtered']]
            return record

        return process(record, mapping)
