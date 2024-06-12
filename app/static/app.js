document.getElementById('upload-form').addEventListener('submit', async function(event) {
  event.preventDefault();
  const fileInput = document.getElementById('file-input');
  const file = fileInput.files[0];
  if (!file) {
      document.getElementById('status').innerText = "ファイルを選択してください。";
      return;
  }

  const formData = new FormData();
  formData.append('file', file);

  try {
      const response = await fetch('/process_csv/', {
          method: 'POST',
          body: formData
      });

      if (response.ok) {
          const blob = await response.blob();
          const downloadUrl = URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = downloadUrl;
          a.download = 'pick_up便設定状況.xlsx';
          document.body.appendChild(a);
          a.click();
          a.remove();
          URL.revokeObjectURL(downloadUrl);
          document.getElementById('status').innerText = "ファイルが正常に処理されました。";
      } else {
          const errorText = await response.text();
          document.getElementById('status').innerText = `エラーが発生しました: ${errorText}`;
      }
  } catch (error) {
      document.getElementById('status').innerText = `エラーが発生しました: ${error.message}`;
  }
});
