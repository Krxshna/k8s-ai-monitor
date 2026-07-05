"""
Kubernetes Cluster Monitor — Temporal-backed agent.

Temporal adds automatic retries, a full execution audit trail, and fault
tolerance on top of the same K8s operations as k8s_agent.py.

Usage:
    # Terminal 1 — start the local Temporal server
    temporal server start-dev

    # Terminal 2 — start the worker (registers activities + workflow)
    python k8s_temporal_agent.py worker

    # Terminal 3 — run the interactive client
    python k8s_temporal_agent.py
"""

import asyncio
import logging
import sys
import uuid
from concurrent .futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio import activity ,workflow
from temporalio .client import Client
from temporalio .common import RetryPolicy
from temporalio .exceptions import ApplicationError
from temporalio .worker import Worker

from config import (
TEMPORAL_HOST ,K8S_TASK_QUEUE ,
OLLAMA_HOST ,OLLAMA_MODEL ,
LIST_PODS_TIMEOUT ,HEALTH_CHECK_TIMEOUT ,
LOG_RETRIEVAL_TIMEOUT ,RESTART_POD_TIMEOUT ,
DEPLOYMENT_CHECK_TIMEOUT ,AI_ORCHESTRATOR_TIMEOUT ,
)

logging .basicConfig (level =logging .INFO ,format ="%(asctime)s %(levelname)s %(message)s")
logger =logging .getLogger (__name__ )


@activity .defn
async def list_pods_activity (namespace :str =None )->str :
    from k8s_client import KubernetesClient ,K8sConnectionError

    activity .logger .info (f"Listing pods — namespace: {namespace or 'all'}")
    try :
        k8s =KubernetesClient ()
        pods =k8s .list_pods (namespace =namespace )
        if not pods :
            scope =f"namespace '{namespace}'"if namespace else "any namespace"
            return f"No pods found in {scope}."

        lines =[f"Found {len(pods)} pod(s):\n"]
        for pod in pods :
            lines .append (pod .format_summary ())
            lines .append ("")
        return "\n".join (lines )

    except K8sConnectionError as e :
        activity .logger .error (f"K8s connection error: {e}")
        raise
    except Exception as e :
        activity .logger .exception ("Unexpected error in list_pods_activity")
        raise ApplicationError (f"Unexpected error: {e}",non_retryable =True )


@activity .defn
async def check_pod_health_activity (pod_name :str =None ,namespace :str ="default")->str :
    from k8s_client import KubernetesClient ,K8sConnectionError ,PodNotFoundError

    activity .logger .info (f"Health check — pod: {pod_name or 'all'}, namespace: {namespace}")
    try :
        k8s =KubernetesClient ()

        if pod_name :
            health =k8s .get_pod_health (pod_name ,namespace )
            return health .format_summary ()

        pods =k8s .list_pods (namespace =namespace )
        if not pods :
            return f"No pods found in namespace '{namespace}'."

        healthy =0
        lines =[f"Health check — {len(pods)} pod(s) in '{namespace}':\n"]
        for pod in pods :
            try :
                h =k8s .get_pod_health (pod .name ,pod .namespace )
                lines .append (h .format_summary ())
                lines .append ("")
                if h .is_healthy :
                    healthy +=1
            except Exception as exc :
                lines .append (f"✗ {pod.name}: error — {exc}\n")

        lines .append (f"Summary: {healthy}/{len(pods)} pods healthy")
        return "\n".join (lines )

    except PodNotFoundError as e :
        raise ApplicationError (f"Pod '{e.pod_name}' not found in '{e.namespace}'",non_retryable =True )
    except K8sConnectionError as e :
        activity .logger .error (f"K8s connection error: {e}")
        raise
    except Exception as e :
        activity .logger .exception ("Unexpected error in check_pod_health_activity")
        raise ApplicationError (f"Unexpected error: {e}",non_retryable =True )


@activity .defn
async def get_pod_logs_activity (pod_name :str ,namespace :str ="default",lines :int =100 )->str :
    from k8s_client import KubernetesClient ,K8sConnectionError ,PodNotFoundError

    activity .logger .info (f"Fetching logs — pod: {pod_name}, namespace: {namespace}, lines: {lines}")
    try :
        k8s =KubernetesClient ()
        logs =k8s .get_pod_logs (pod_name ,namespace ,lines =lines )
        if not logs :
            return f"No logs available for pod '{pod_name}' in '{namespace}'."

        header =f"Last {lines} lines — {pod_name} [{namespace}]"
        return f"{header}\n{'─' * len(header)}\n{logs}"

    except PodNotFoundError as e :
        raise ApplicationError (f"Pod '{e.pod_name}' not found in '{e.namespace}'",non_retryable =True )
    except K8sConnectionError as e :
        activity .logger .error (f"K8s connection error: {e}")
        raise
    except Exception as e :
        activity .logger .exception ("Unexpected error in get_pod_logs_activity")
        raise ApplicationError (f"Unexpected error: {e}",non_retryable =True )


@activity .defn
async def restart_pod_activity (pod_name :str ,namespace :str ="default")->str :
    from k8s_client import KubernetesClient ,K8sConnectionError ,PodNotFoundError

    activity .logger .info (f"Restarting pod — {pod_name} in {namespace}")
    try :
        k8s =KubernetesClient ()
        k8s .delete_pod (pod_name ,namespace )
        return (
        f"Pod '{pod_name}' deleted from namespace '{namespace}'. "
        "Its controller is spinning up a fresh replacement."
        )
    except PodNotFoundError as e :
        raise ApplicationError (f"Pod '{e.pod_name}' not found in '{e.namespace}'",non_retryable =True )
    except K8sConnectionError as e :
        activity .logger .error (f"K8s connection error: {e}")
        raise
    except Exception as e :
        activity .logger .exception ("Unexpected error in restart_pod_activity")
        raise ApplicationError (f"Unexpected error: {e}",non_retryable =True )


@activity .defn
async def get_deployment_status_activity (name :str ,namespace :str ="default")->str :
    from k8s_client import KubernetesClient ,K8sConnectionError ,DeploymentNotFoundError

    activity .logger .info (f"Checking deployment — {name} in {namespace}")
    try :
        k8s =KubernetesClient ()
        dep =k8s .get_deployment (name ,namespace )
        return dep .format_summary ()

    except DeploymentNotFoundError as e :
        raise ApplicationError (f"Deployment '{e.name}' not found in '{e.namespace}'",non_retryable =True )
    except K8sConnectionError as e :
        activity .logger .error (f"K8s connection error: {e}")
        raise
    except Exception as e :
        activity .logger .exception ("Unexpected error in get_deployment_status_activity")
        raise ApplicationError (f"Unexpected error: {e}",non_retryable =True )


@activity .defn
async def list_nodes_activity ()->str :
    from k8s_client import KubernetesClient ,K8sConnectionError

    activity .logger .info ("Listing cluster nodes")
    try :
        k8s =KubernetesClient ()
        nodes =k8s .list_nodes ()
        if not nodes :
            return "No nodes found in the cluster."

        ready_count =sum (1 for n in nodes if n .status =="Ready")
        lines =[f"Cluster nodes — {ready_count}/{len(nodes)} ready:\n"]
        for node in nodes :
            lines .append (node .format_summary ())
            lines .append ("")
        return "\n".join (lines )

    except K8sConnectionError as e :
        activity .logger .error (f"K8s connection error: {e}")
        raise
    except Exception as e :
        activity .logger .exception ("Unexpected error in list_nodes_activity")
        raise ApplicationError (f"Unexpected error: {e}",non_retryable =True )


@activity .defn
async def find_resource_activity (keyword :str )->str :
    """Search pods and deployments by name keyword across all namespaces."""
    from k8s_client import KubernetesClient ,K8sConnectionError

    activity .logger .info (f"Searching resources — keyword: {keyword}")
    try :
        k8s =KubernetesClient ()
        return k8s .search_resources (keyword )
    except K8sConnectionError as e :
        activity .logger .error (f"K8s connection error: {e}")
        raise
    except Exception as e :
        activity .logger .exception ("Unexpected error in find_resource_activity")
        raise ApplicationError (f"Unexpected error: {e}",non_retryable =True )


@activity .defn
async def smart_logs_activity (keyword :str ,namespace :str =None ,lines :int =100 )->str :
    """Find the first pod matching keyword and return its logs."""
    from k8s_client import KubernetesClient ,K8sConnectionError

    activity .logger .info (f"Smart logs — keyword: {keyword}, namespace: {namespace}, lines: {lines}")
    try :
        k8s =KubernetesClient ()
        return k8s .get_pod_logs_by_keyword (keyword ,namespace =namespace ,lines =lines )
    except K8sConnectionError as e :
        activity .logger .error (f"K8s connection error: {e}")
        raise
    except Exception as e :
        activity .logger .exception ("Unexpected error in smart_logs_activity")
        raise ApplicationError (f"Unexpected error: {e}",non_retryable =True )


def _keyword_fallback (task :str )->str :
    """
    Last-resort routing when the LLM returns unusable output.
    Matches common intent patterns directly from the user's words.
    """
    t =task .lower ()

    if any (w in t for w in ("log","logs","tail","output","stdout")):
        words =t .split ()
        for i ,w in enumerate (words ):
            if w in ("log","logs","of","from","for")and i +1 <len (words ):
                keyword =words [i +1 ].strip ("?.,")
                if len (keyword )>2 :
                    return f"smart_logs:{keyword}"
        return "pods"

    if any (phrase in t for phrase in ("is there","do we have","exists","find","any ")):
        candidates =[w for w in t .split ()if len (w )>3
        and w not in ("there","have","find","does","this","that","with","show")]
        return f"find:{candidates[-1]}"if candidates else "pods"

    if any (w in t for w in ("node","nodes","worker")):
        return "nodes"

    if any (w in t for w in ("health","healthy","unhealthy","running ok")):
        return "health"

    if any (w in t for w in ("restart","bounce","kill")):
        return "pods"

    if any (w in t for w in ("deploy","deployment","replica")):
        return "pods"

    return "pods"


@activity .defn
async def ai_orchestrator_activity (task :str )->str :
    """
    Translate a natural-language request into a comma-separated operation plan.
    Calls Ollama directly via HTTP (not through Strands Agent) so the model
    returns plain text without tool-use wrapper interference.

    Operations:
        find:keyword          search pods+deployments by name
        smart_logs:keyword    find pod by keyword and get its logs
        pods                  list all pods
        pods:namespace        list pods in one namespace
        health                health-check all pods
        health:pod:ns         health-check one pod
        logs:pod:ns           exact pod logs (full pod name required)
        logs:pod:ns:N         last N lines
        restart:pod:ns        delete pod to trigger restart
        deploy:name:ns        check Deployment replica status
        nodes                 list cluster nodes
    """
    import re
    import requests as req

    activity .logger .info (f"AI orchestrator — task: {task!r}")

    system_prompt ="""You are a Kubernetes query router. Reply with ONE LINE only — a comma-separated list of operations. No explanation. No markdown. No extra text.

Operations:
  find:keyword          search for pods/deployments by name keyword
  smart_logs:keyword    get logs from pod matching keyword (use when user says "logs of X" and X is an app name, not exact pod name)
  pods                  list all pods
  pods:namespace        list pods in one namespace
  health                health-check all pods in default namespace
  health:pod:ns         health-check one specific pod (exact pod name)
  logs:pod:ns           get logs (exact pod name required)
  logs:pod:ns:N         get last N lines (exact pod name required)
  restart:pod:ns        restart a pod (exact pod name required)
  deploy:name:ns        check deployment replica status
  nodes                 list cluster nodes

Rules:
- "logs of X" or "show X logs" when X is an app name → smart_logs:X
- "logs of X" when X is an exact pod name → logs:X:namespace
- "is there X", "find X", "any X", "do we have X" → find:X
- "restart X" → restart:X:namespace
- "health of X" → health:X:namespace
- "deployment X" → deploy:X:namespace
- "nodes" → nodes
- Default namespace is "default"

Examples (write exactly like this — nothing else):
show logs of notes-app → smart_logs:notes-app
is there any notes-app? → find:notes-app
list all pods → pods
health of coredns in kube-system → health:coredns:kube-system
restart api-server in staging → restart:api-server:staging
check web deployment → deploy:web:default
are nodes ready? → nodes
show running pods in dev → pods:dev"""

    try :
        resp =req .post (
        f"{OLLAMA_HOST}/api/chat",
        json ={
        "model":OLLAMA_MODEL ,
        "messages":[
        {"role":"system","content":system_prompt },
        {"role":"user","content":task },
        ],
        "stream":False ,
        "options":{"temperature":0 },
        },
        timeout =30 ,
        )
        resp .raise_for_status ()
        raw =resp .json ()["message"]["content"].strip ()
        activity .logger .info (f"Model raw output: {raw!r}")


        plan =None
        for line in raw .splitlines ():
            line =line .strip ().strip ("`").strip ()
            if line and re .match (r"^[a-z]",line )and len (line )<300 :
                plan =line
                break

        if not plan :
            activity .logger .warning ("Model returned unusable output, using keyword fallback")
            plan =_keyword_fallback (task )

    except Exception as e :
        activity .logger .warning (f"Ollama call failed ({e}), using keyword fallback")
        plan =_keyword_fallback (task )

    activity .logger .info (f"Resolved plan: {plan!r}")
    return plan


ALL_ACTIVITIES =[
list_pods_activity ,
check_pod_health_activity ,
get_pod_logs_activity ,
restart_pod_activity ,
get_deployment_status_activity ,
list_nodes_activity ,
find_resource_activity ,
smart_logs_activity ,
ai_orchestrator_activity ,
]




@workflow .defn
class K8sMonitorWorkflow :
    """
    Temporal workflow for Kubernetes cluster monitoring.

    Execution order:
      1. ai_orchestrator_activity  → parses the user's query into an operation plan
      2. One or more K8s activities → each executed with its own retry policy
      3. Results assembled and returned as a single string
    """

    @workflow .run
    async def run (self ,task :str )->str :
        workflow .logger .info (f"K8sMonitorWorkflow started — task: {task!r}")


        plan =await workflow .execute_activity (
        ai_orchestrator_activity ,
        task ,
        start_to_close_timeout =timedelta (seconds =AI_ORCHESTRATOR_TIMEOUT ),
        retry_policy =RetryPolicy (
        maximum_attempts =2 ,
        initial_interval =timedelta (seconds =1 ),
        maximum_interval =timedelta (seconds =2 ),
        backoff_coefficient =1.0 ,
        ),
        )

        workflow .logger .info (f"Executing plan: {plan}")

        results =[f"[AI plan: {plan}]\n"]

        for spec in [s .strip ()for s in plan .split (",")if s .strip ()]:
            try :
                result =await self ._dispatch (spec )
                results .append (result )
            except Exception as exc :
                workflow .logger .error (f"Operation '{spec}' failed: {exc}")
                results .append (f"Operation '{spec}' failed: {exc}")

        return "\n\n".join (results )

    async def _dispatch (self ,spec :str )->str :
        """Route a single operation spec to the right activity."""
        parts =spec .split (":")
        op =parts [0 ].lower ()
        p1 =parts [1 ]if len (parts )>1 else None
        p2 =parts [2 ]if len (parts )>2 else None
        p3 =parts [3 ]if len (parts )>3 else None

        if op =="find":
            if not p1 :
                return "Error: find operation requires a keyword (find:keyword)"
            return await workflow .execute_activity (
            find_resource_activity ,
            p1 ,
            start_to_close_timeout =timedelta (seconds =LIST_PODS_TIMEOUT ),
            retry_policy =RetryPolicy (
            maximum_attempts =3 ,
            initial_interval =timedelta (seconds =1 ),
            maximum_interval =timedelta (seconds =5 ),
            backoff_coefficient =2.0 ,
            ),
            )

        if op =="smart_logs":
            if not p1 :
                return "Error: smart_logs requires a keyword (smart_logs:keyword)"
            namespace =p2 if p2 and not p2 .isdigit ()else None
            lines =int (p2 )if p2 and p2 .isdigit ()else (int (p3 )if p3 and p3 .isdigit ()else 100 )
            return await workflow .execute_activity (
            smart_logs_activity ,
            args =[p1 ,namespace ,lines ],
            start_to_close_timeout =timedelta (seconds =LOG_RETRIEVAL_TIMEOUT ),
            retry_policy =RetryPolicy (
            maximum_attempts =2 ,
            initial_interval =timedelta (seconds =1 ),
            maximum_interval =timedelta (seconds =5 ),
            backoff_coefficient =2.0 ,
            ),
            )

        if op =="pods":
            return await workflow .execute_activity (
            list_pods_activity ,
            p1 ,
            start_to_close_timeout =timedelta (seconds =LIST_PODS_TIMEOUT ),
            retry_policy =RetryPolicy (
            maximum_attempts =3 ,
            initial_interval =timedelta (seconds =1 ),
            maximum_interval =timedelta (seconds =5 ),
            backoff_coefficient =2.0 ,
            ),
            )

        if op =="health":
            pod_name =p1
            namespace =p2 or "default"
            return await workflow .execute_activity (
            check_pod_health_activity ,
            args =[pod_name ,namespace ],
            start_to_close_timeout =timedelta (seconds =HEALTH_CHECK_TIMEOUT ),
            retry_policy =RetryPolicy (
            maximum_attempts =3 ,
            initial_interval =timedelta (seconds =2 ),
            maximum_interval =timedelta (seconds =10 ),
            backoff_coefficient =2.0 ,
            ),
            )

        if op =="logs":
            if not p1 :
                return "Error: logs operation requires a pod name (logs:pod:namespace)"
            namespace =p2 or "default"
            lines =int (p3 )if p3 else 100
            return await workflow .execute_activity (
            get_pod_logs_activity ,
            args =[p1 ,namespace ,lines ],
            start_to_close_timeout =timedelta (seconds =LOG_RETRIEVAL_TIMEOUT ),
            retry_policy =RetryPolicy (
            maximum_attempts =2 ,
            initial_interval =timedelta (seconds =1 ),
            maximum_interval =timedelta (seconds =5 ),
            backoff_coefficient =2.0 ,
            ),
            )

        if op =="restart":
            if not p1 :
                return "Error: restart operation requires a pod name (restart:pod:namespace)"
            namespace =p2 or "default"
            return await workflow .execute_activity (
            restart_pod_activity ,
            args =[p1 ,namespace ],
            start_to_close_timeout =timedelta (seconds =RESTART_POD_TIMEOUT ),
            retry_policy =RetryPolicy (
            maximum_attempts =5 ,
            initial_interval =timedelta (seconds =3 ),
            maximum_interval =timedelta (seconds =30 ),
            backoff_coefficient =2.0 ,
            ),
            )

        if op =="deploy":
            if not p1 :
                return "Error: deploy operation requires a deployment name (deploy:name:namespace)"
            namespace =p2 or "default"
            return await workflow .execute_activity (
            get_deployment_status_activity ,
            args =[p1 ,namespace ],
            start_to_close_timeout =timedelta (seconds =DEPLOYMENT_CHECK_TIMEOUT ),
            retry_policy =RetryPolicy (
            maximum_attempts =3 ,
            initial_interval =timedelta (seconds =1 ),
            maximum_interval =timedelta (seconds =5 ),
            backoff_coefficient =2.0 ,
            ),
            )

        if op =="nodes":
            return await workflow .execute_activity (
            list_nodes_activity ,
            start_to_close_timeout =timedelta (seconds =LIST_PODS_TIMEOUT ),
            retry_policy =RetryPolicy (
            maximum_attempts =3 ,
            initial_interval =timedelta (seconds =1 ),
            maximum_interval =timedelta (seconds =5 ),
            backoff_coefficient =2.0 ,
            ),
            )

        return f"Unknown operation: '{op}'"




async def run_worker ():
    print (f"Connecting to Temporal at {TEMPORAL_HOST} ...")
    client =await Client .connect (TEMPORAL_HOST )
    print (f"Worker listening on queue: {K8S_TASK_QUEUE}")
    print ("Press Ctrl+C to stop\n")

    worker =Worker (
    client ,
    task_queue =K8S_TASK_QUEUE ,
    workflows =[K8sMonitorWorkflow ],
    activities =ALL_ACTIVITIES ,
    activity_executor =ThreadPoolExecutor (max_workers =5 ),
    )
    await worker .run ()




async def run_client ():
    print ("="*55 )
    print ("Kubernetes Cluster Monitor — Temporal Agent")
    print ("="*55 )

    try :
        print (f"Connecting to Temporal at {TEMPORAL_HOST} ...")
        client =await Client .connect (TEMPORAL_HOST )
        print ("Connected\n")
    except Exception as exc :
        print (f"Could not connect to Temporal: {exc}")
        print ("Start it with:  temporal server start-dev")
        return

    print ("Try asking:")
    print ("  - List all pods")
    print ("  - Check health of the nginx pod in production")
    print ("  - Show me the last 50 lines from api-server in default")
    print ("  - Restart my-app pod in staging")
    print ("  - Status of the web deployment")
    print ("  - Are all nodes ready?")
    print ("\nType 'quit' to exit")
    print ("="*55 +"\n")

    while True :
        try :
            task =input ("Ask: ").strip ()
            if not task :
                continue
            if task .lower ()in ("quit","q","exit"):
                print ("Goodbye!")
                break

            workflow_id =f"k8s-monitor-{uuid.uuid4()}"
            print ("Processing ...\n")

            result =await client .execute_workflow (
            K8sMonitorWorkflow .run ,
            task ,
            id =workflow_id ,
            task_queue =K8S_TASK_QUEUE ,
            )
            print (f"{result}\n")

        except KeyboardInterrupt :
            print ("\nGoodbye!")
            break
        except Exception as exc :
            print (f"Error: {exc}\n")




if __name__ =="__main__":
    mode =sys .argv [1 ]if len (sys .argv )>1 else "client"
    if mode =="worker":
        asyncio .run (run_worker ())
    else :
        asyncio .run (run_client ())
