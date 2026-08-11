import type { Theme } from "@/lib/theme";

import baseLight from "@syncfusion/ej2-base/styles/material.css?url";
import baseDark from "@syncfusion/ej2-base/styles/material-dark.css?url";
import buttonsLight from "@syncfusion/ej2-buttons/styles/material.css?url";
import buttonsDark from "@syncfusion/ej2-buttons/styles/material-dark.css?url";
import inputsLight from "@syncfusion/ej2-inputs/styles/material.css?url";
import inputsDark from "@syncfusion/ej2-inputs/styles/material-dark.css?url";
import popupsLight from "@syncfusion/ej2-popups/styles/material.css?url";
import popupsDark from "@syncfusion/ej2-popups/styles/material-dark.css?url";
import icons from "@syncfusion/ej2-icons/styles/material.css?url";
import navigationsLight from "@syncfusion/ej2-navigations/styles/material.css?url";
import navigationsDark from "@syncfusion/ej2-navigations/styles/material-dark.css?url";
import layoutsLight from "@syncfusion/ej2-layouts/styles/material.css?url";
import layoutsDark from "@syncfusion/ej2-layouts/styles/material-dark.css?url";
import gridsLight from "@syncfusion/ej2-grids/styles/material.css?url";
import gridsDark from "@syncfusion/ej2-grids/styles/material-dark.css?url";
import splitbuttonsLight from "@syncfusion/ej2-splitbuttons/styles/material.css?url";
import splitbuttonsDark from "@syncfusion/ej2-splitbuttons/styles/material-dark.css?url";
import dropdownsLight from "@syncfusion/ej2-dropdowns/styles/material.css?url";
import dropdownsDark from "@syncfusion/ej2-dropdowns/styles/material-dark.css?url";
import filemanagerLight from "@syncfusion/ej2-react-filemanager/styles/material.css?url";
import filemanagerDark from "@syncfusion/ej2-react-filemanager/styles/material-dark.css?url";

// ej2-icons ships no dark variant (font glyphs are theme-invariant).
const STYLESHEETS: { light: string; dark: string }[] = [
    { light: baseLight, dark: baseDark },
    { light: buttonsLight, dark: buttonsDark },
    { light: inputsLight, dark: inputsDark },
    { light: popupsLight, dark: popupsDark },
    { light: icons, dark: icons },
    { light: navigationsLight, dark: navigationsDark },
    { light: layoutsLight, dark: layoutsDark },
    { light: gridsLight, dark: gridsDark },
    { light: splitbuttonsLight, dark: splitbuttonsDark },
    { light: dropdownsLight, dark: dropdownsDark },
    { light: filemanagerLight, dark: filemanagerDark },
];

// The Syncfusion FileManager can't be themed with CSS variables, so the whole
// material / material-dark stylesheet set is swapped when the theme flips.
// Links live on document.head (never removed on unmount: the page may remount
// and dialogs/menus portal to <body>).
export function applySyncfusionTheme(theme: Theme) {
    STYLESHEETS.forEach((sheet, index) => {
        const id = `syncfusion-theme-${index}`;
        let link = document.getElementById(id) as HTMLLinkElement | null;
        if (!link) {
            link = document.createElement("link");
            link.id = id;
            link.rel = "stylesheet";
            document.head.appendChild(link);
        }
        const href = theme === "dark" ? sheet.dark : sheet.light;
        if (link.getAttribute("href") !== href) link.setAttribute("href", href);
    });
}
