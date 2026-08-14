import { ReactNode } from "react";

interface PageHeaderProps {
    title: string;
    // Rendered inline after the title (status badge, etc.)
    titleAccessory?: ReactNode;
    description?: ReactNode;
    // Right-aligned actions (buttons)
    actions?: ReactNode;
}

export const PageHeader = ({ title, titleAccessory, description, actions }: PageHeaderProps) => (
    <div className="flex flex-wrap items-start justify-between gap-x-6 gap-y-3 pb-6">
        <div className="min-w-0">
            <div className="flex items-center gap-3">
                <h1 className="truncate text-xl font-semibold tracking-tight text-foreground">
                    {title}
                </h1>
                {titleAccessory}
            </div>
            {description && (
                <p className="mt-1 text-[13px] text-muted-foreground">{description}</p>
            )}
        </div>
        {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
    </div>
);
