#### Main Service

The "main service" is a fastapi webservice designed to be deployed in [google-cloud-run](cloud.google.com/run).  
This service acts as a traditional "head node" would, as well as handing other responsibilities.  
This service is responsible for:

- Adding/removing/managing nodes in the cluster.
- Routing requests from clients to the correct `node_service`'s, (`/burla/node_service`)
- Hosting the cluster-management dashboard (react/ts)

Every "main service" instance has it's own [google-cloud-firestore](cloud.google.com/firestore) database associated with it.  
It is currently not possible to run more than one "main-service" instance in any single google-cloud-project.  
It is currently not possible to run more than one "cluster" using a single "main-service".  

#### Dev:

To avoid the need for CORS middleware I use a script that builds the react website every time I hit save. It takes about the same amount of time to build as the fastapi webservice takes to reload, so it dosent actually slow anything down much.  
To get this setup install the vscode extension called "Run on Save", the publisher is "emeraldwalk". After installing add the following to your `settings.json` (open this by hitting `Cmd + Shift + P`, then type `Preferences: Open Settings (JSON)` and select it):
```json
{
    // <other settings you've set will be here, add below to the main dict>
    "emeraldwalk.runonsave": {
        "commands": [
            {
                "match": ".*", // Run whenever any file is saved
                "cmd": "make -C ./main_service build-frontend"
            }
        ]
    }
}
```
Now the website should build everytime you hit save! (It should take <2s to build)  
To see the output of this command press `Cmd + Shift + U`, then select `Run on Save` in the dropdown.