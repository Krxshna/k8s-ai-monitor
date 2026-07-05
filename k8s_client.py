"""Kubernetes client wrapper — structured data types and consistent error handling."""

import logging
from dataclasses import dataclass ,field
from datetime import datetime ,timezone
from typing import List ,Optional ,Dict

from kubernetes import client ,config
from kubernetes .client .rest import ApiException

logger =logging .getLogger (__name__ )




class K8sConnectionError (Exception ):
    """Raised when unable to reach the Kubernetes API server."""
    pass


class PodNotFoundError (Exception ):
    def __init__ (self ,pod_name :str ,namespace :str ):
        self .pod_name =pod_name
        self .namespace =namespace
        super ().__init__ (f"Pod '{pod_name}' not found in namespace '{namespace}'")


class DeploymentNotFoundError (Exception ):
    def __init__ (self ,name :str ,namespace :str ):
        self .name =name
        self .namespace =namespace
        super ().__init__ (f"Deployment '{name}' not found in namespace '{namespace}'")




@dataclass
class PodInfo :
    """Snapshot of a single pod."""
    name :str
    namespace :str
    phase :str
    ready :str
    restarts :int
    node :str
    age :str
    labels :Dict [str ,str ]=field (default_factory =dict )

    def format_summary (self )->str :
        status_icon ="✓"if self .phase =="Running"else "✗"
        return (
        f"{status_icon} {self.name}  [{self.namespace}]\n"
        f"   Phase:    {self.phase}\n"
        f"   Ready:    {self.ready}\n"
        f"   Restarts: {self.restarts}\n"
        f"   Node:     {self.node}\n"
        f"   Age:      {self.age}"
        )


@dataclass
class PodHealthStatus :
    """Detailed health assessment of a pod."""
    name :str
    namespace :str
    phase :str
    is_healthy :bool
    containers_ready :int
    containers_total :int
    restart_count :int
    issues :List [str ]=field (default_factory =list )

    def format_summary (self )->str :
        icon ="✓"if self .is_healthy else "✗"
        label ="Healthy"if self .is_healthy else "Unhealthy"
        lines =[
        f"{icon} {self.name}  [{self.namespace}] — {label}",
        f"   Phase:      {self.phase}",
        f"   Containers: {self.containers_ready}/{self.containers_total} ready",
        f"   Restarts:   {self.restart_count}",
        ]
        if self .issues :
            lines .append (f"   Issues:     {', '.join(self.issues)}")
        return "\n".join (lines )


@dataclass
class DeploymentInfo :
    """Status of a Kubernetes Deployment."""
    name :str
    namespace :str
    desired :int
    ready :int
    available :int
    up_to_date :int

    def format_summary (self )->str :
        healthy =self .ready ==self .desired
        icon ="✓"if healthy else "✗"
        return (
        f"{icon} Deployment: {self.name}  [{self.namespace}]\n"
        f"   Desired:    {self.desired}\n"
        f"   Ready:      {self.ready}\n"
        f"   Available:  {self.available}\n"
        f"   Up-to-date: {self.up_to_date}"
        )


@dataclass
class NodeInfo :
    """Summary of a cluster node."""
    name :str
    status :str
    roles :str
    age :str
    kubelet_version :str

    def format_summary (self )->str :
        icon ="✓"if self .status =="Ready"else "✗"
        return (
        f"{icon} {self.name}  ({self.roles})\n"
        f"   Status:  {self.status}\n"
        f"   Version: {self.kubelet_version}\n"
        f"   Age:     {self.age}"
        )




class KubernetesClient :
    """
    Thin wrapper around the official Kubernetes Python SDK.
    Automatically loads kubeconfig for local dev or in-cluster config when
    running inside a pod.
    """

    def __init__ (self ):
        try :
            try :
                config .load_kube_config ()
                logger .info ("Loaded kubeconfig from local file")
            except config .ConfigException :
                config .load_incluster_config ()
                logger .info ("Loaded in-cluster Kubernetes config")

            self .core =client .CoreV1Api ()
            self .apps =client .AppsV1Api ()

            self .core .list_namespace (limit =1 )
            logger .info ("Successfully connected to Kubernetes API server")

        except ApiException as e :
            raise K8sConnectionError (f"Kubernetes API error during init: {e}")from e
        except Exception as e :
            raise K8sConnectionError (f"Could not connect to Kubernetes: {e}")from e



    def list_pods (self ,namespace :str =None )->List [PodInfo ]:
        """Return pods across all namespaces or a specific one."""
        try :
            if namespace :
                raw =self .core .list_namespaced_pod (namespace )
            else :
                raw =self .core .list_pod_for_all_namespaces ()
            return [self ._to_pod_info (p )for p in raw .items ]
        except ApiException as e :
            raise K8sConnectionError (f"Failed to list pods: {e}")from e

    def get_pod_health (self ,pod_name :str ,namespace :str ="default")->PodHealthStatus :
        """Assess the health of a single pod."""
        try :
            pod =self .core .read_namespaced_pod (pod_name ,namespace )
            return self ._assess_pod_health (pod )
        except ApiException as e :
            if e .status ==404 :
                raise PodNotFoundError (pod_name ,namespace )
            raise K8sConnectionError (f"API error checking pod health: {e}")from e

    def get_pod_logs (self ,pod_name :str ,namespace :str ="default",lines :int =100 )->str :
        """Fetch recent logs from the first container of a pod."""
        try :
            logs =self .core .read_namespaced_pod_log (
            pod_name ,
            namespace ,
            tail_lines =lines ,
            timestamps =True ,
            )
            return logs or ""
        except ApiException as e :
            if e .status ==404 :
                raise PodNotFoundError (pod_name ,namespace )
            raise K8sConnectionError (f"Failed to fetch logs for '{pod_name}': {e}")from e

    def get_pod_logs_by_keyword (self ,keyword :str ,namespace :str =None ,lines :int =100 )->str :
        """
        Find the first pod whose name contains keyword and return its logs.
        Searches across all namespaces when namespace is None.
        """
        try :
            raw =(
            self .core .list_namespaced_pod (namespace )
            if namespace
            else self .core .list_pod_for_all_namespaces ()
            )
            matches =[
            p for p in raw .items
            if keyword .lower ()in p .metadata .name .lower ()
            ]
        except ApiException as e :
            raise K8sConnectionError (f"Failed to search pods: {e}")from e

        if not matches :
            return f"No pod found whose name contains '{keyword}'."

        pod =matches [0 ]
        pod_name =pod .metadata .name
        pod_ns =pod .metadata .namespace
        header =f"Logs from {pod_name} [{pod_ns}] — last {lines} lines"
        logs =self .get_pod_logs (pod_name ,pod_ns ,lines )
        if not logs :
            return f"No logs available for pod '{pod_name}' in '{pod_ns}'."
        return f"{header}\n{'─' * len(header)}\n{logs}"

    def delete_pod (self ,pod_name :str ,namespace :str ="default")->bool :
        """
        Delete a pod so Kubernetes recreates it via its controller.
        Returns True when the delete API call succeeds.
        """
        try :
            self .core .delete_namespaced_pod (pod_name ,namespace )
            logger .info (f"Deleted pod {pod_name} in {namespace} — controller will recreate it")
            return True
        except ApiException as e :
            if e .status ==404 :
                raise PodNotFoundError (pod_name ,namespace )
            raise K8sConnectionError (f"Failed to delete pod '{pod_name}': {e}")from e



    def get_deployment (self ,name :str ,namespace :str ="default")->DeploymentInfo :
        """Fetch replica counts for a named Deployment."""
        try :
            dep =self .apps .read_namespaced_deployment (name ,namespace )
            return DeploymentInfo (
            name =dep .metadata .name ,
            namespace =dep .metadata .namespace ,
            desired =dep .spec .replicas or 0 ,
            ready =dep .status .ready_replicas or 0 ,
            available =dep .status .available_replicas or 0 ,
            up_to_date =dep .status .updated_replicas or 0 ,
            )
        except ApiException as e :
            if e .status ==404 :
                raise DeploymentNotFoundError (name ,namespace )
            raise K8sConnectionError (f"Failed to get deployment '{name}': {e}")from e



    def search_resources (self ,keyword :str )->str :
        """
        Case-insensitive keyword search across pods and deployments in all namespaces.
        Returns a combined summary of matches.
        """
        keyword_lower =keyword .lower ()
        results =[]

        try :
            all_pods =self .core .list_pod_for_all_namespaces ()
            matched_pods =[
            p for p in all_pods .items
            if keyword_lower in p .metadata .name .lower ()
            or keyword_lower in p .metadata .namespace .lower ()
            ]
            if matched_pods :
                results .append (f"Pods matching '{keyword}':\n")
                for p in matched_pods :
                    results .append (self ._to_pod_info (p ).format_summary ())
                    results .append ("")
        except ApiException as e :
            results .append (f"Could not search pods: {e}")

        try :
            all_deps =self .apps .list_deployment_for_all_namespaces ()
            matched_deps =[
            d for d in all_deps .items
            if keyword_lower in d .metadata .name .lower ()
            or keyword_lower in d .metadata .namespace .lower ()
            ]
            if matched_deps :
                results .append (f"Deployments matching '{keyword}':\n")
                for d in matched_deps :
                    info =DeploymentInfo (
                    name =d .metadata .name ,
                    namespace =d .metadata .namespace ,
                    desired =d .spec .replicas or 0 ,
                    ready =d .status .ready_replicas or 0 ,
                    available =d .status .available_replicas or 0 ,
                    up_to_date =d .status .updated_replicas or 0 ,
                    )
                    results .append (info .format_summary ())
                    results .append ("")
        except ApiException as e :
            results .append (f"Could not search deployments: {e}")

        if not results :
            return f"No pods or deployments found matching '{keyword}'."

        return "\n".join (results )



    def list_nodes (self )->List [NodeInfo ]:
        """Return a summary of every node in the cluster."""
        try :
            raw =self .core .list_node ()
            return [self ._to_node_info (n )for n in raw .items ]
        except ApiException as e :
            raise K8sConnectionError (f"Failed to list nodes: {e}")from e



    def _to_pod_info (self ,pod )->PodInfo :
        ready_count =0
        total_count =0
        restarts =0

        if pod .status .container_statuses :
            for cs in pod .status .container_statuses :
                total_count +=1
                if cs .ready :
                    ready_count +=1
                restarts +=cs .restart_count or 0

        age =self ._compute_age (pod .metadata .creation_timestamp )

        return PodInfo (
        name =pod .metadata .name ,
        namespace =pod .metadata .namespace ,
        phase =pod .status .phase or "Unknown",
        ready =f"{ready_count}/{total_count}",
        restarts =restarts ,
        node =pod .spec .node_name or "unscheduled",
        age =age ,
        labels =pod .metadata .labels or {},
        )

    def _assess_pod_health (self ,pod )->PodHealthStatus :
        issues =[]
        phase =pod .status .phase or "Unknown"
        ready_count =0
        total_count =0
        restarts =0

        if pod .status .container_statuses :
            for cs in pod .status .container_statuses :
                total_count +=1
                if cs .ready :
                    ready_count +=1
                restarts +=cs .restart_count or 0


                if cs .state and cs .state .waiting :
                    reason =cs .state .waiting .reason or ""
                    if reason in ("CrashLoopBackOff","ImagePullBackOff","ErrImagePull","OOMKilled"):
                        issues .append (f"{cs.name}: {reason}")
        else :
            total_count =len (pod .spec .containers )

        if phase not in ("Running","Succeeded"):
            issues .append (f"Phase is {phase}")

        from config import POD_RESTART_THRESHOLD
        if restarts >=POD_RESTART_THRESHOLD :
            issues .append (f"High restart count ({restarts})")

        if ready_count <total_count and phase =="Running":
            issues .append (f"Only {ready_count}/{total_count} containers ready")

        is_healthy =phase in ("Running","Succeeded")and ready_count ==total_count and not issues

        return PodHealthStatus (
        name =pod .metadata .name ,
        namespace =pod .metadata .namespace ,
        phase =phase ,
        is_healthy =is_healthy ,
        containers_ready =ready_count ,
        containers_total =total_count ,
        restart_count =restarts ,
        issues =issues ,
        )

    def _to_node_info (self ,node )->NodeInfo :
        status ="Unknown"
        for condition in (node .status .conditions or []):
            if condition .type =="Ready":
                status ="Ready"if condition .status =="True"else "NotReady"
                break

        roles =[]
        for label in (node .metadata .labels or {}):
            if label .startswith ("node-role.kubernetes.io/"):
                roles .append (label .split ("/")[-1 ])
        role_str =", ".join (roles )if roles else "worker"

        kubelet_version =""
        if node .status .node_info :
            kubelet_version =node .status .node_info .kubelet_version

        return NodeInfo (
        name =node .metadata .name ,
        status =status ,
        roles =role_str ,
        age =self ._compute_age (node .metadata .creation_timestamp ),
        kubelet_version =kubelet_version ,
        )

    @staticmethod
    def _compute_age (creation_timestamp )->str :
        if not creation_timestamp :
            return "unknown"
        now =datetime .now (timezone .utc )
        delta =now -creation_timestamp
        seconds =int (delta .total_seconds ())
        if seconds <3600 :
            return f"{seconds // 60}m"
        if seconds <86400 :
            return f"{seconds // 3600}h"
        return f"{seconds // 86400}d"
