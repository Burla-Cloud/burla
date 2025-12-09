import React from "react";
import {
    FileManagerComponent,
    Inject,
    NavigationPane,
    DetailsView,
    Toolbar,
} from "@syncfusion/ej2-react-filemanager";

// Syncfusion styles required for File Manager UI
import "@syncfusion/ej2-base/styles/material.css";
import "@syncfusion/ej2-buttons/styles/material.css";
import "@syncfusion/ej2-inputs/styles/material.css";
import "@syncfusion/ej2-popups/styles/material.css";
import "@syncfusion/ej2-icons/styles/material.css";
import "@syncfusion/ej2-navigations/styles/material.css";
import "@syncfusion/ej2-layouts/styles/material.css";
import "@syncfusion/ej2-grids/styles/material.css";
import "@syncfusion/ej2-splitbuttons/styles/material.css";
import "@syncfusion/ej2-dropdowns/styles/material.css";
import "@syncfusion/ej2-react-filemanager/styles/material.css";

export default function Storage() {
    return (
        <div className="w-full h-full">
            <FileManagerComponent
                id="gcs-file-manager"
                height="800px"
                ajaxSettings={{
                    url: "/api/sf/filemanager",
                    uploadUrl: "/api/sf/upload",
                    downloadUrl: "/api/sf/download",
                }}
                rootAliasName="/"
            >
                <Inject services={[NavigationPane, DetailsView, Toolbar]} />
            </FileManagerComponent>
        </div>
    );
}
