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
    ]

    async execute(request: Hub.ActionRequest) {

        try {

            if (!request.params.project_id || !request.formParams.reqion || !request.formParams.dataset_id) {
                return new Hub.ActionResponse({ success: false, message: "project_id region and dataset are mandatory" })
            }

            await this.pushFileToGoogleBucket(request)
            const client = this.getAutomlInstance(request)
            const bucket_location = `gs://${request.params.project_id}/${request.params.file_name}`

            const ml_request = {
                name: client.datasetPath(request.params.project_id, request.formParams.reqion, request.formParams.dataset_id),
                inputConfig: {
                    gcsSource: {
                        inputUris: [bucket_location],
                    },
                },
            };

            const [operation] = await client.importData(ml_request);
            // Wait for operation to complete.
            await operation.promise();
            return new Hub.ActionResponse({ success: true })

        } catch (e) {
            return new Hub.ActionResponse({ success: false, message: e.message })
        }
    }

    async form(request: Hub.ActionRequest) {
        const form = new Hub.ActionForm()

        try {

            form.fields = [
                {
                    name: "dataset_id",
                    label: "Dataset Id",
                    required: true,
                    description: "The name of the new dataset (display name)",
                }, {
                    name: "region",
                    label: "Region",
                    required: true,
                    description: "Region where the data set will be created",
                }, {
                    name: "file_name",
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
            form.error = e.message
            return form
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
            if (form.error != "") {
                throw form.error
            }

            return form.fields.find(field => field.name == "bucket")

        } catch (e) {

            throw e
        }
    }
}

Hub.addAction(new GoogleAutomlDataSet())