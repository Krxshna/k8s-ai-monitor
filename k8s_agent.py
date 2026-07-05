"""
Kubernetes Cluster Monitor — Simple Strands agent.

Run:
    python k8s_agent.py

Requires a reachable cluster (kubeconfig or in-cluster) and Ollama running locally.
"""

import json
import logging
import sys

from strands import Agent ,tool
from strands .models .ollama import OllamaModel



from config import (
OLLAMA_HOST ,OLLAMA_MODEL ,
K8S_DEFAULT_NAMESPACE ,POD_RESTART_THRESHOLD ,
)
from k8s_client import (
KubernetesClient ,
K8sConnectionError ,
PodNotFoundError ,
DeploymentNotFoundError ,
)

logging .basicConfig (level =logging .INFO ,format ="%(asctime)s %(levelname)s %(message)s")
logger =logging .getLogger (__name__ )


try :
    _k8s =KubernetesClient ()
except K8sConnectionError as e :
    logger .error (f"Kubernetes init failed: {e}")
    _k8s =None


def _no_cluster ()->str :
    return "Cannot reach the Kubernetes API. Check your kubeconfig or cluster connection."


def format_agent_result (result )->str :
    """Format an AgentResult for CLI output, including toolResult blocks."""
    if hasattr (result ,"structured_output")and result .structured_output is not None :
        return result .structured_output .model_dump_json (indent =2 )

    message =getattr (result ,"message",{})or {}
    content =message .get ("content",[])
    lines :list [str ]=[]

    for block in content :
        if isinstance (block ,dict ):
            if "text"in block :
                lines .append (block ["text"])
            elif "toolResult"in block :
                tool_result =block ["toolResult"]
                if isinstance (tool_result ,dict ):
                    if tool_result .get ("status"):
                        lines .append (f"Tool status: {tool_result['status']}")
                    tool_content =tool_result .get ("content",[])
                    if isinstance (tool_content ,list ):
                        for item in tool_content :
                            if isinstance (item ,dict ):
                                if "text"in item :
                                    lines .append (item ["text"])
                                elif "json"in item :
                                    lines .append (json .dumps (item ["json"],indent =2 ))
                                else :
                                    lines .append (str (item ))
                            else :
                                lines .append (str (item ))
                    else :
                        lines .append (str (tool_content ))
                else :
                    lines .append (str (tool_result ))
            elif "citationsContent"in block :
                citations_block =block ["citationsContent"]
                if "content"in citations_block :
                    for content_item in citations_block ["content"]:
                        if isinstance (content_item ,dict )and "text"in content_item :
                            lines .append (content_item ["text"])
            elif "json"in block :
                lines .append (json .dumps (block ["json"],indent =2 ))
            else :
                lines .append (str (block ))
        else :
            lines .append (str (block ))

    formatted ="\n".join (line for line in lines if line ).strip ()
    if formatted :
        return formatted


    try :
        return json .dumps (message ,indent =2 )
    except Exception :
        return str (message )




@tool
def list_pods (namespace :str =None )->str :
    """
    List all pods in a namespace or across all namespaces.

    Args:
        namespace: The Kubernetes namespace to list pods from.
                  Leave empty (None) to list pods from all namespaces.
                  Default: None (all namespaces)

    Returns:
        A formatted string listing all pods with their status.
    """
    if _k8s is None :
        return _no_cluster ()
    try :
        pods =_k8s .list_pods (namespace =namespace )
        if not pods :
            scope =f"namespace '{namespace}'"if namespace else "any namespace"
            return f"No pods found in {scope}."

        lines =[f"Found {len(pods)} pod(s):\n"]
        for pod in pods :
            lines .append (pod .format_summary ())
            lines .append ("")
        return "\n".join (lines )

    except K8sConnectionError :
        return _no_cluster ()
    except Exception :
        logger .exception ("list_pods failed")
        return "Unexpected error listing pods. Check logs for details."


@tool (description ="Count pods in a namespace or across all namespaces.")
def count_pods (namespace :str =None )->str :
    """Return the total number of pods in a namespace or across the cluster."""
    if _k8s is None :
        return _no_cluster ()
    try :
        pods =_k8s .list_pods (namespace =namespace )
        if not pods :
            scope =f"namespace '{namespace}'"if namespace else "the cluster"
            return f"No pods found in {scope}."

        if namespace :
            return f"Found {len(pods)} pod(s) in namespace '{namespace}'."
        return f"Found {len(pods)} pod(s) across all namespaces."

    except K8sConnectionError :
        return _no_cluster ()
    except Exception :
        logger .exception ("count_pods failed")
        return "Unexpected error counting pods. Check logs for details."


@tool
def check_pod_health (pod_name :str =None ,namespace :str ="default")->str :
    """
    Check the health status of a specific pod or all pods in a namespace.

    Args:
        pod_name: The exact name of the pod to check. Leave empty (None) to check all pods.
                 Default: None (check all pods)
        namespace: The Kubernetes namespace containing the pod.
                  Default: "default"

    Returns:
        Health status summary for the pod(s).
    """
    if _k8s is None :
        return _no_cluster ()
    try :
        if pod_name :
            health =_k8s .get_pod_health (pod_name ,namespace )
            return health .format_summary ()

        pods =_k8s .list_pods (namespace =namespace )
        if not pods :
            return f"No pods found in namespace '{namespace}'."

        healthy_count =0
        lines =[f"Health check — {len(pods)} pod(s) in '{namespace}':\n"]
        for pod in pods :
            try :
                h =_k8s .get_pod_health (pod .name ,pod .namespace )
                lines .append (h .format_summary ())
                lines .append ("")
                if h .is_healthy :
                    healthy_count +=1
            except Exception as exc :
                lines .append (f"✗ {pod.name}: error — {exc}")
                lines .append ("")

        lines .append (f"Summary: {healthy_count}/{len(pods)} pods healthy")
        return "\n".join (lines )

    except PodNotFoundError as e :
        return f"Pod '{e.pod_name}' not found in namespace '{e.namespace}'."
    except K8sConnectionError :
        return _no_cluster ()
    except Exception :
        logger .exception ("check_pod_health failed")
        return "Unexpected error during health check. Check logs for details."


@tool
def get_logs_by_name (keyword :str ,namespace :str =None ,lines :int =100 )->str :
    """
    Fetch logs from the first pod whose name contains the keyword.
    Use this when the user says 'logs of notes-app' or 'show X logs' and
    you don't know the exact full pod name. Searches all namespaces by default.
    """
    if _k8s is None :
        return _no_cluster ()
    try :
        return _k8s .get_pod_logs_by_keyword (keyword ,namespace =namespace ,lines =lines )
    except K8sConnectionError :
        return _no_cluster ()
    except Exception :
        logger .exception ("get_logs_by_name failed")
        return "Unexpected error fetching logs. Check logs for details."


@tool
def get_pod_logs (pod_name :str ,namespace :str ="default",lines :int =100 )->str :
    """Fetch the last N log lines from a pod using its exact full name."""
    if _k8s is None :
        return _no_cluster ()
    try :
        logs =_k8s .get_pod_logs (pod_name ,namespace ,lines =lines )
        if not logs :
            return f"No logs available for pod '{pod_name}' in '{namespace}'."

        header =f"Last {lines} lines — {pod_name} [{namespace}]"
        return f"{header}\n{'─' * len(header)}\n{logs}"

    except PodNotFoundError as e :
        return f"Pod '{e.pod_name}' not found in namespace '{e.namespace}'."
    except K8sConnectionError :
        return _no_cluster ()
    except Exception :
        logger .exception ("get_pod_logs failed")
        return "Unexpected error retrieving logs. Check logs for details."


@tool
def restart_pod (pod_name :str ,namespace :str ="default")->str :
    """
    Restart a pod by deleting it.
    Kubernetes automatically recreates it through the owning controller
    (Deployment, ReplicaSet, StatefulSet, etc.).
    """
    if _k8s is None :
        return _no_cluster ()
    try :
        _k8s .delete_pod (pod_name ,namespace )
        return (
        f"Pod '{pod_name}' deleted from namespace '{namespace}'. "
        "Its controller is spinning up a fresh replacement."
        )
    except PodNotFoundError as e :
        return f"Pod '{e.pod_name}' not found in namespace '{e.namespace}'."
    except K8sConnectionError :
        return _no_cluster ()
    except Exception :
        logger .exception ("restart_pod failed")
        return "Unexpected error restarting pod. Check logs for details."


@tool
def get_deployment_status (deployment_name :str ,namespace :str ="default")->str :
    """Check replica health of a named Deployment."""
    if _k8s is None :
        return _no_cluster ()
    try :
        dep =_k8s .get_deployment (deployment_name ,namespace )
        return dep .format_summary ()

    except DeploymentNotFoundError as e :
        return f"Deployment '{e.name}' not found in namespace '{e.namespace}'."
    except K8sConnectionError :
        return _no_cluster ()
    except Exception :
        logger .exception ("get_deployment_status failed")
        return "Unexpected error fetching deployment. Check logs for details."


@tool
def find_resource (keyword :str )->str :
    """
    Search for pods and deployments by name keyword across all namespaces.

    Args:
        keyword: The search keyword or partial name. Examples: 'notes-app', 'nginx', 'redis'

    Returns:
        A list of all matching pods and deployments.
    """
    if _k8s is None :
        return _no_cluster ()
    try :
        return _k8s .search_resources (keyword )
    except K8sConnectionError :
        return _no_cluster ()
    except Exception :
        logger .exception ("find_resource failed")
        return "Unexpected error during search. Check logs for details."


@tool
def list_nodes ()->str :
    """Show every node in the cluster with its readiness and Kubernetes version."""
    if _k8s is None :
        return _no_cluster ()
    try :
        nodes =_k8s .list_nodes ()
        if not nodes :
            return "No nodes found in the cluster."

        ready_count =sum (1 for n in nodes if n .status =="Ready")
        lines =[f"Cluster nodes — {ready_count}/{len(nodes)} ready:\n"]
        for node in nodes :
            lines .append (node .format_summary ())
            lines .append ("")
        return "\n".join (lines )

    except K8sConnectionError :
        return _no_cluster ()
    except Exception :
        logger .exception ("list_nodes failed")
        return "Unexpected error listing nodes. Check logs for details."




def create_agent ()->Agent :
    model =OllamaModel (host =OLLAMA_HOST ,model_id =OLLAMA_MODEL )




    return Agent (
    model =model ,
    tools =[find_resource ,get_logs_by_name ,list_pods ,count_pods ,check_pod_health ,get_pod_logs ,restart_pod ,get_deployment_status ,list_nodes ],
    system_prompt =(
    "You are a Kubernetes cluster operations assistant. "
    "Always respond with tool calls to inspect pods, count pods, check health, read logs, restart pods, "
    "examine deployments, and list nodes. "
    "\n\nWhen the user asks about pods, deployments, nodes, or logs:\n"
    "1. If asking about pod counts, 'number of pods', or 'how many pods' → call count_pods with the namespace if provided\n"
    "2. If asking about 'all pods' or 'pods in a namespace' → call list_pods with the namespace\n"
    "3. If asking 'is X healthy?' → call check_pod_health with the pod name\n"
    "4. If asking 'logs of X' or 'show X logs' → call get_logs_by_name with the app keyword\n"
    "5. If asking about 'nodes' → call list_nodes\n"
    "6. If asking to 'find X' or 'is there X' → call find_resource with the keyword\n"
    "7. If asking about a deployment → call get_deployment_status with the deployment name\n"
    "\nAlways use a tool first before answering. "
    "Be concise. If something looks wrong, say so clearly and suggest a next step."
    ),
    )




def main ():
    banner ="Kubernetes Cluster Monitor"
    print ("="*50 )
    print (banner )
    print ("="*50 )

    if _k8s is None :
        print ("\nERROR: Could not connect to Kubernetes API.")
        print ("Make sure your kubeconfig is set up and the cluster is reachable.")
        print ("  export KUBECONFIG=~/.kube/config")
        sys .exit (1 )

    print (f"\nConnected to cluster  |  model: {OLLAMA_MODEL} via {OLLAMA_HOST}")
    print ("\nTry asking:")
    print ("  - List all pods")
    print ("  - Is the nginx pod healthy?")
    print ("  - Show me logs from my-app in the staging namespace")
    print ("  - How many nodes are ready?")
    print ("  - What's the status of the web deployment?")
    print ("\nType 'quit' to exit")
    print ("="*50 +"\n")

    agent =create_agent ()

    while True :
        try :
            query =input ("Ask: ").strip ()
            if not query :
                continue
            if query .lower ()in ("quit","q","exit"):
                print ("Goodbye!")
                break

            print ("Thinking...\n")
            result =agent (query )
            output =format_agent_result (result )
            print (f"\n{output}\n")

        except KeyboardInterrupt :
            print ("\nGoodbye!")
            break
        except Exception :
            logger .exception ("Agent error")
            print ("Something went wrong. Check the logs above.\n")


if __name__ =="__main__":
    main ()
