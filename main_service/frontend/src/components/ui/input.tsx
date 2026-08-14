import * as React from "react";

import { cn } from "@/lib/utils";

const Input = React.forwardRef<HTMLInputElement, React.ComponentProps<"input">>(
    ({ className, type, ...props }, ref) => {
        return (
            <input
                type={type}
                className={cn(
                    "flex h-9 w-full rounded-lg border border-input bg-card px-3 py-1 text-sm text-foreground shadow-sm transition-[border-color,box-shadow] duration-150 file:border-0 file:bg-transparent file:text-sm file:font-medium file:text-foreground placeholder:text-muted-foreground/70 focus-visible:border-primary/70 focus-visible:outline-none focus-visible:ring-[3px] focus-visible:ring-primary/15 disabled:cursor-not-allowed disabled:bg-muted/50 disabled:opacity-60",
                    className
                )}
                ref={ref}
                {...props}
            />
        );
    }
);
Input.displayName = "Input";

export { Input };
