import * as chai from "chai"
import * as sinon from "sinon"
// import { Stream } from "stream"

import * as Hub from "../../../hub"

import { GoogleAutomlTable } from "./google_auto_ml_table"
import { GoogleCloudStorageAction } from "../gcs/google_cloud_storage"

const action = new GoogleAutomlTable()

describe(`${action.constructor.name} unit tests`, () => {

    describe("form", () => {
        it("has form", () => {
            chai.expect(action.hasForm).equals(true)
        })

        it("error missing config", (done) => {
            const request = new Hub.ActionRequest()
            request.params = {
                client_email: "foo",
                private_key: "foo",
            }

            const form = action.validateAndFetchForm(request)
            chai.expect(form).to.eventually
                .deep.eq({ error: `Required setting "Project Id" not specified in action settings.`, fields: [] })
                .and.notify(done)
        })

        it("error loading datasets", (done) => {
            const stubClient = sinon.stub(action as any, "getAutomlInstance")
                .callsFake(() => ({
                    listDatasets: async () => Promise.resolve([]),
                    locationPath: () => "parent",
                }))

            const request = new Hub.ActionRequest()
            request.params = {
                client_email: "foo",
                private_key: "foo",
                project_id: "foo",
                region: "foo",
            }

            const form = action.validateAndFetchForm(request)
            chai.expect(form).to.eventually
                .deep.eq({ error: "error populating form fields: Error: no datasets found in this account", fields: [] })
                .and.notify(stubClient.restore)
                .and.notify(done)
        })

        it("error loading buckets", (done) => {
            const stubClient = sinon.stub(action as any, "getAutomlInstance")
                .callsFake(() => ({
                    listDatasets: async () => Promise.resolve([[{ name: "ds", displayName: "ds" }]]),
                    locationPath: () => "parent",
                }))

            const gcsForm = new Hub.ActionForm()
            gcsForm.error = "error fetching buckets"
            const stubGCSAction = sinon.stub(GoogleCloudStorageAction.prototype, "validateAndFetchForm")
                .resolves(gcsForm)

            const request = new Hub.ActionRequest()
            request.params = {
                client_email: "foo",
                private_key: "foo",
                project_id: "foo",
                region: "foo",
            }

            const form = action.validateAndFetchForm(request)
            chai.expect(form).to.eventually
                .deep.eq({
                    error: "error populating form fields: error fetching buckets", fields: [
                        {
                            name: "dataset_id",
                            label: "Dataset",
                            required: true,
                            options: [{ name: "ds", label: "ds" }],
                            type: "select",
                            default: "ds",
                        },
                        {
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
                        }
                    ]
                })
                .and.notify(stubClient.restore)
                .and.notify(stubGCSAction.restore)
                .and.notify(done)
        })

        it("form loads well", (done) => {
            const stubClient = sinon.stub(action as any, "getAutomlInstance")
                .callsFake(() => ({
                    listDatasets: async () => Promise.resolve([[{ name: "ds", displayName: "ds" }]]),
                    locationPath: () => "parent",
                }))

            const gcsForm = new Hub.ActionForm()
            gcsForm.fields = [{
                label: "Bucket",
                name: "bucket",
                required: true,
                options: [{ name: 'bucket', label: 'bucket' }],
                type: "select",
                default: 'bucket',
            }]

            const stubGCSAction = sinon.stub(GoogleCloudStorageAction.prototype, "validateAndFetchForm")
                .resolves(gcsForm)

            const request = new Hub.ActionRequest()
            request.params = {
                client_email: "foo",
                private_key: "foo",
                project_id: "foo",
                region: "foo",
            }

            const form = action.validateAndFetchForm(request)
            chai.expect(form).to.eventually
                .deep.eq({
                    fields: [
                        {
                            name: "dataset_id",
                            label: "Dataset",
                            required: true,
                            options: [{ name: "ds", label: "ds" }],
                            type: "select",
                            default: "ds",
                        },
                        {
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
                        {
                            label: "Bucket",
                            name: "bucket",
                            required: true,
                            options: [{ name: 'bucket', label: 'bucket' }],
                            type: "select",
                            default: 'bucket',
                        }
                    ]
                })
                .and.notify(stubClient.restore)
                .and.notify(stubGCSAction.restore)
                .and.notify(done)
        })
    })

    describe("execute", () => {

        it("error no params", () => {
            const request = new Hub.ActionRequest()
            request.type = Hub.ActionType.Dashboard
            request.params = {
                client_email: "foo",
                private_key: "foo",
                project_id: "foo",
                region: "foo",
            }
            request.formParams = {
                filename: "filename",
                overwrite: "yes",
            }
            request.attachment = {}
            request.attachment.dataBuffer = Buffer.from("1,2,3,4", "utf8")
            const response = action.validateAndExecute(request)
            return chai.expect(response).to.eventually
                .deep.equal({
                    success: false,
                    message: "project_id, region and dataset are mandatory",
                    refreshQuery: false,
                    validationErrors: []
                })
        })

        it("error pushing gile to gcs", () => {
            const request = new Hub.ActionRequest()
            request.type = Hub.ActionType.Dashboard
            request.params = {
                client_email: "foo",
                private_key: "foo",
                project_id: "foo",
                region: "foo",
            }
            request.formParams = {
                filename: "filename",
                overwrite: "yes",
                dataset_id: "ds",
            }
            request.attachment = {}
            request.attachment.dataBuffer = Buffer.from("1,2,3,4", "utf8")

            const stubGCSAction = sinon.stub(GoogleCloudStorageAction.prototype, "validateAndExecute")
                .rejects(new Error("error pushing file to the fake gcs"))

            const response = action.validateAndExecute(request)
            return chai.expect(response).to.eventually
                .deep.equal(
                    {
                        success: false,
                        message: "error pushing file to the fake gcs",
                        refreshQuery: false,
                        validationErrors: []
                    })
                .and.notify(stubGCSAction.restore)
        })

        it("error importing data", () => {
            const request = new Hub.ActionRequest()
            request.type = Hub.ActionType.Dashboard
            request.params = {
                client_email: "foo",
                private_key: "foo",
                project_id: "foo",
                region: "foo",
            }
            request.formParams = {
                filename: "filename",
                overwrite: "yes",
                dataset_id: "ds",
            }
            request.attachment = {}
            request.attachment.dataBuffer = Buffer.from("1,2,3,4", "utf8")

            const stubGCSAction = sinon.stub(GoogleCloudStorageAction.prototype, "validateAndExecute")
                .resolves(new Error("pushed file to gcss"))

            const stubClient = sinon.stub(action as any, "getAutomlInstance")
                .callsFake(() => ({
                    importData: async () => Promise.reject(new Error("error importing ds")),
                }))

            const response = action.validateAndExecute(request)
            return chai.expect(response).to.eventually
                .deep.equal(
                    {
                        success: false,
                        message: "error importing ds",
                        refreshQuery: false,
                        validationErrors: []
                    })
                .and.notify(stubGCSAction.restore)
                .and.notify(stubClient.restore)
        })

        it("excecution ok", () => {
            const request = new Hub.ActionRequest()
            request.type = Hub.ActionType.Dashboard
            request.params = {
                client_email: "foo",
                private_key: "foo",
                project_id: "foo",
                region: "foo",
            }
            request.formParams = {
                filename: "filename",
                overwrite: "yes",
                dataset_id: "ds",
            }
            request.attachment = {}
            request.attachment.dataBuffer = Buffer.from("1,2,3,4", "utf8")

            const stubGCSAction = sinon.stub(GoogleCloudStorageAction.prototype, "validateAndExecute")
                .resolves(new Error("pushed file to gcss"))

            const stubClient = sinon.stub(action as any, "getAutomlInstance")
                .callsFake(() => ({
                    importData: async () => Promise.resolve([{ promise: () => { } }]),
                }))

            const response = action.validateAndExecute(request)
            return chai.expect(response).to.eventually
                .deep.equal(
                    {
                        success: true,
                        refreshQuery: false,
                        validationErrors: []
                    })
                .and.notify(stubGCSAction.restore)
                .and.notify(stubClient.restore)
        })

    })
})