import { Button } from "@/components/ui/button";

interface TablePaginationProps {
    page: number; // 0-based
    totalPages: number;
    onPageChange: (page: number) => void;
    // e.g. "23 nodes" / "112 results"; falls back to page position.
    resultsLabel?: string;
}

// Stripe-style list footer: count on the left, Previous / Next on the right.
export const TablePagination = ({
    page,
    totalPages,
    onPageChange,
    resultsLabel,
}: TablePaginationProps) => (
    <div className="flex items-center justify-between pt-4">
        <span className="text-[13px] text-muted-foreground">
            {resultsLabel ?? `Page ${page + 1} of ${Math.max(1, totalPages)}`}
        </span>
        <div className="flex items-center gap-2">
            {resultsLabel && totalPages > 1 && (
                <span className="mr-1 text-[13px] tabular-nums text-muted-foreground">
                    Page {page + 1} of {totalPages}
                </span>
            )}
            <Button
                variant="outline"
                size="sm"
                disabled={page === 0}
                onClick={() => onPageChange(page - 1)}
            >
                Previous
            </Button>
            <Button
                variant="outline"
                size="sm"
                disabled={page >= totalPages - 1}
                onClick={() => onPageChange(page + 1)}
            >
                Next
            </Button>
        </div>
    </div>
);
