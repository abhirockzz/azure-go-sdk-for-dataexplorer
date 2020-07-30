package ops

import (
	"context"
	"log"

	"github.com/Azure/azure-sdk-for-go/services/kusto/mgmt/2020-02-15/kusto"
)

// ListClusters lists all ADX clusters in a resource group
func ListClusters(sub, rgName string) kusto.ClusterListResult {
	ctx := context.Background()

	result, err := getClustersClient(sub).ListByResourceGroup(ctx, rgName)
	if err != nil {
		log.Fatal(err.Error())
	}
	return result
}

// CreateCluster creates an ADX cluster: 1 instance of compute type DevNoSLAStandardD11V2 with Basic tier
func CreateCluster(sub, clusterName, location, rgName string) {

	ctx := context.Background()

	numInstances := int32(1)
	client := getClustersClient(sub)
	result, err := client.CreateOrUpdate(ctx, rgName, clusterName, kusto.Cluster{Location: &location, Sku: &kusto.AzureSku{Name: kusto.DevNoSLAStandardD11V2, Capacity: &numInstances, Tier: kusto.Basic}})

	if err != nil {
		log.Fatal("failed to start cluster creation ", err)
	}

	log.Println("waiting for cluster creation to complete....")
	err = result.WaitForCompletionRef(context.Background(), client.Client)
	if err != nil {
		log.Fatal("error during cluster creation", err)
	}

	r, err := result.Result(client)
	if err != nil {
		log.Fatal("cluster creation process has not yet completed", err)
	}

	log.Printf("created cluster %s with ID %s and type %s", *r.Name, *r.ID, *r.Type)
}

// DeleteCluster deletes an ADX cluster in a resource group
func DeleteCluster(sub, clusterName, rgName string) {

	ctx := context.Background()

	client := getClustersClient(sub)
	result, err := client.Delete(ctx, rgName, clusterName)

	if err != nil {
		log.Fatal("failed to start cluster deletion ", err)
	}

	log.Println("waiting for cluster deletion to complete....")
	err = result.WaitForCompletionRef(context.Background(), client.Client)
	if err != nil {
		log.Fatal("error during cluster deletion", err)
	}

	r, err := result.Result(client)
	if err != nil {
		log.Fatal("cluster deletion process has not yet completed", err)
	}

	if r.StatusCode == 200 {
		log.Printf("deleted ADX cluster %s from resource group %s", clusterName, rgName)
	} else {
		log.Println("failed to delete ADX cluster. response status code -", r.StatusCode)
	}
}
