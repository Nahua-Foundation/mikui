export function Tab({ title, active = false, onClick }: { title: string; active?: boolean; onClick?: () => void }) {
  return (
    <div
      className="box-border content-stretch flex flex-row gap-2.5 items-center justify-center px-2 py-4 relative shrink-0 cursor-pointer"
      data-name="tab"
      onClick={onClick}
    >
      <div className={`font-mono font-[450] leading-[0] relative shrink-0 text-[16px] text-left text-nowrap ${
        active ? 'text-strong' : 'text-soft hover:text-strong'
      }`}>
        <p className="block leading-[24px] whitespace-pre">{title}</p>
      </div>
    </div>
  );
}