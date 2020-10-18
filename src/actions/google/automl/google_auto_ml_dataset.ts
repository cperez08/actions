import * as Hub from "../../../hub"

import { AutoMlClient } from '@google-cloud/automl'
import { GoogleCloudStorageAction } from "../gcs/google_cloud_storage"

export class GoogleAutomlDataSet extends Hub.Action {

    name = "google_automl"
    label = "Google Cloud AutomML"
    iconName = "google/automl/google_automl.png"
    description = "Import your data into a Google AutoML"
    supportedActionTypes = [Hub.ActionType.Dashboard, Hub.ActionType.Query]
    usesStreaming = true
    requiredFields = []
    params = [
        {
            name: "client_email",
            label: "Client Email",
            required: true,
            sensitive: false,
            description: "Your client email for GCS from https://console.cloud.google.com/apis/credentials",
        }, {
            name: "private_key",
            label: "Private Key",
            required: true,
            sensitive: true,
            description: "Your private key for GCS from https://console.cloud.google.com/apis/credentials",
        }, {
            name: "project_id",
            label: "Project Id",
            required: true,
            sensitive: false,
            description: "The Project Id for your GCS project from https://console.cloud.google.com/apis/credentials",
        },
        {
            name: "region",
            label: "Region",
            required: true,
            sensitive: false,
            description: "the region will be used to manage the datasets (us-central1)",
        },

    ]

    async execute(request: Hub.ActionRequest) {

        try {
            if (!request.params.project_id || !request.params.region || !request.formParams.dataset_id) {
                return new Hub.ActionResponse({ success: false, message: "project_id region and dataset are mandatory" })
            }

            console.time("push_file_gcs");
            await this.pushFileToGoogleBucket(request)
            console.timeEnd("push_file_gcs");

            const client = this.getAutomlInstance(request)
            const bucket_location = `gs://${request.formParams.bucket}/${request.formParams.filename}`

            const ml_request = {
                name: request.formParams.dataset_id,
                inputConfig: {
                    gcsSource: {
                        inputUris: [bucket_location],
                    },
                },
            };


            console.time("import_data_automl");
            const [operation] = await client.importData(ml_request);
            console.timeEnd("import_data_automl");
            operation.promise();
            return new Hub.ActionResponse({ success: true })

        } catch (e) {
            console.log(`error importing dataset: ${e}`)
            return new Hub.ActionResponse({ success: false, message: e.message })
        }
    }

    async form(request: Hub.ActionRequest) {

        const form = new Hub.ActionForm()

        try {
            const datasets = await this.getDatasetList(request)
            form.fields = [
                {
                    name: "dataset_id",
                    label: "Dataset",
                    required: true,
                    options: datasets,
                    type: "select",
                    default: datasets[0].name,
                }, {
                    name: "filename",
                    label: "File Name",
                    required: true,
                    description: "the name of the file that will be created in the Google storage",
                },
                {
                    name: "overwrite",
                    label: "Overwrite",
                    options: [{ label: "Yes", name: "yes" }, { label: "No", name: "no" }],
                    default: "yes",
                    description: "If Overwrite is enabled, will use the title or filename and overwrite existing data." +
                        " If disabled, a date time will be appended to the name to make the file unique.",
                },
            ]

            const buckets = await this.getBucketList(request)
            form.fields.push(<Hub.ActionFormField>buckets)

        } catch (e) {
            form.error = `error populating form fields: ${e}`
        }

        return form
    }

    private getAutomlInstance(request: Hub.ActionRequest) {

        const credentials = {
            client_email: request.params.client_email,
            private_key: request.params.private_key!.replace(/\\n/gm, "\n"),
        }

        const config = {
            projectId: request.params.project_id,
            credentials,
        }

        return new AutoMlClient(config)
    }

    private async pushFileToGoogleBucket(request: Hub.ActionRequest) {

        try {
            const storage_action = new GoogleCloudStorageAction()
            await storage_action.validateAndExecute(request)
        } catch (e) {
            throw e
        }
    }

    private async getBucketList(request: Hub.ActionRequest) {

        try {
            const storage_action = new GoogleCloudStorageAction()
            const form = await storage_action.validateAndFetchForm(request)

            if (form.error) {
                throw form.error
            }

            return form.fields.find(field => field.name == "bucket")
        } catch (e) {
            throw e
        }
    }

    private async getDatasetList(request: Hub.ActionRequest) {

        if (!request.params.project_id || !request.params.region) {
            throw Error('project id and region are required')
        }

        const client = this.getAutomlInstance(request)
        const list_request = {
            parent: client.locationPath(request.params.project_id, request.params.region),
        };

        const [results] = await client.listDatasets(list_request)

        if (!results) {
            throw Error('no datasets found in this account')
        }

        return results.map((b: any) => {
            return { name: b.name, label: b.displayName }
        })
    }
}

Hub.addAction(new GoogleAutomlDataSet())