const diagram = document.querySelector('.pv-pairing');
if (diagram) {
  const controls = diagram.querySelector('.pv-controls');
  const descriptions = {
    same: {
      title: 'Two crops of each satellite observation are paired',
      caption: 'Same-observation training pairs A1 with A2, and B1 with B2. Both crops in a pair come from the same acquisition.'
    },
    dates: {
      title: 'Crops from repeat satellite observations are paired',
      caption: 'Repeat-observation training pairs A1 with B2, and B1 with A2. Both pairs cross dates, while using the same four input crops.'
    }
  };
  controls.hidden = false;
  for (const button of controls.querySelectorAll('button')) {
    button.addEventListener('click', () => {
      const rule = button.dataset.rule;
      diagram.dataset.pairing = rule;
      for (const choice of controls.querySelectorAll('button')) {
        choice.setAttribute('aria-pressed', String(choice === button));
      }
      diagram.querySelector('#pv-pair-title').textContent = descriptions[rule].title;
      diagram.querySelector('#pv-pair-caption').textContent = descriptions[rule].caption;
    });
  }
}
