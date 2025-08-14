export function MenuItem({ children, borderSide = "right" }: { children: React.ReactNode; borderSide?: "left" | "right" | "both" | "none" }) {
  return (
    <div
      className="box-border content-stretch flex flex-col items-start justify-start p-0 relative shrink-0"
      data-name="menu item"
    >
      {children}
    </div>
  );
}