function levenshteinDistance(a, b) {
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;

  const matrix = [];

  for (let i = 0; i <= b.length; i++) {
    matrix[i] = [i];
  }

  for (let j = 0; j <= a.length; j++) {
    matrix[0][j] = j;
  }

  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) === a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        );
      }
    }
  }
  return matrix[b.length][a.length];
}

function spellcheck(input, dictionary, maxEditDistance = 1) {
  let closestMatch = null;
  let minEditDistance = Number.MAX_VALUE;

  for (const word of dictionary) {
    const distance = levenshteinDistance(input, word);
    if (distance < minEditDistance && distance <= maxEditDistance) {
      minEditDistance = distance;
      closestMatch = word;
    }
  }
  return closestMatch;
}

export const autocomplete = (inp, arr) => {
  var currentFocus;

  inp.addEventListener("keydown", function (e) {
    var x = document.getElementById(this.id + "autocomplete-list");
    if (x) x = x.getElementsByTagName("div");
    if (e.keyCode == 40) {
      currentFocus++;
      addActive(x);
    } else if (e.keyCode == 38) {
      currentFocus--;
      addActive(x);
    } else if (e.keyCode == 13) {
      e.preventDefault();
      if (currentFocus > -1) {
        if (x) x[currentFocus].click();
      }
    }
  });

  inp.addEventListener("input", function () {
    const inputVal = this.value;

    if (!inputVal) {
      closeAllLists();
      return false;
    }

    const spellcheckResult = spellcheck(inputVal, arr);
    console.log("spellcheckResult: " + spellcheckResult);
    if (spellcheckResult) {
      closeAllLists();
      currentFocus = -1;
      var a = document.createElement("DIV");
      a.setAttribute("id", inp.id + "autocomplete-list");
      a.setAttribute("class", "autocomplete-items");
      inp.parentNode.appendChild(a);

      const b = document.createElement("DIV");
      b.innerHTML = "Do you mean: <strong>" + spellcheckResult + "</strong>?";
      b.innerHTML += "<input type='hidden' value='" + spellcheckResult + "'>";
      b.addEventListener("click", function (e) {
        inp.value = this.getElementsByTagName("input")[0].value;
        closeAllLists();
      });
      a.appendChild(b);
    } else {
      if (this.value.length === 0) {
        closeAllLists();
      }
    }
  });

  closeAllLists();

  currentFocus = -1;
  var a = document.createElement("DIV");
  a.setAttribute("id", inp.id + "autocomplete-list");
  a.setAttribute("class", "autocomplete-items");
  inp.parentNode.appendChild(a);

  for (let i = 0; i < Math.min(10, arr.length); i++) {
    var b = document.createElement("DIV");
    b.innerHTML = "<strong>" + arr[i].substr(0, inp.value.length) + "</strong>";
    b.innerHTML += arr[i].substr(inp.value.length);
    b.innerHTML += "<input type='hidden' value='" + arr[i] + "'>";
    b.addEventListener("click", function (e) {
      inp.value = this.getElementsByTagName("input")[0].value;
      closeAllLists();
    });
    a.appendChild(b);
  }

  function addActive(x) {
    if (!x) return false;
    removeActive(x);
    if (currentFocus >= x.length) currentFocus = 0;
    if (currentFocus < 0) currentFocus = x.length - 1;
    x[currentFocus].classList.add("autocomplete-active");
  }

  function removeActive(x) {
    for (var i = 0; i < x.length; i++) {
      x[i].classList.remove("autocomplete-active");
    }
  }

  function closeAllLists(elmnt) {
    var x = document.getElementsByClassName("autocomplete-items");
    for (var i = 0; i < x.length; i++) {
      if (elmnt != x[i] && elmnt != inp) {
        x[i].parentNode.removeChild(x[i]);
      }
    }
  }

  document.addEventListener("click", function (e) {
    closeAllLists(e.target);
  });
};