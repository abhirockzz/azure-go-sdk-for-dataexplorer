package ops

import (
	"log"

	"github.com/Azure/azure-sdk-for-go/services/kusto/mgmt/2020-02-15/kusto"
	"github.com/Azure/go-autorest/autorest/azure/auth"
)

func getClustersClient(subscription string) kusto.ClustersClient {
	client := kusto.NewClustersClient(subscription)
	authR, err := auth.NewAuthorizerFromEnvironment()
	if err != nil {
		log.Fatal(err)
	}
	client.Authorizer = authR

	return client
}

func getDBClient(subscription string) kusto.DatabasesClient {
	client := kusto.NewDatabasesClient(subscription)
	authR, err := auth.NewAuthorizerFromEnvironment()
	if err != nil {
		log.Fatal(err)
	}
	client.Authorizer = authR

	return client
}
