#### Main Service

The "main service" is a fastapi webservice deployed as a single always-on VM (the cluster's "head node"), on GCE or EC2.  
This service is responsible for:

- Adding/removing/managing nodes in the cluster (via a compute-provider interface, GCP or AWS).
- Holding the cluster's live coordination state in memory (nodes push state to it over HTTP every ~1s).
- Persisting job/node history to SQLite on its disk (`/var/lib/burla/history.db`) for the dashboard.
- Hosting the cluster-management dashboard (react/ts)

There is no external database. The head is a stateful singleton:  
It is currently not possible to run more than one "main-service" instance in any single cloud account.  
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
                "match": "frontend/src/.*\\.(js|ts|jsx|tsx|css|scss|html)$", // Run whenever any source-code file is saved
                "cmd": "make -C ./main_service build-frontend"
            }
        ]
    }
}
```
Now the website should build everytime you hit save! (It should take <2s to build)  
To see the output of this command press `Cmd + Shift + U`, then select `Run on Save` in the dropdown.
