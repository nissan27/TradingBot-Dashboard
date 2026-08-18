export default function Home() {
  return (
    <main className="dashboard-frame">
      <iframe
        className="dashboard-preview"
        src="/dashboard.html?preview=1"
        title="TradingBot operational dashboard"
      />
    </main>
  );
}
