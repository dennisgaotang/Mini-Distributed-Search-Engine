export const createTitle = (titleText, link) => {
  const title = document.createElement("div");
  const titleLink = document.createElement("a");
  const content = document.createTextNode(titleText);
  title.classList.add("title");
  title.appendChild(titleLink);
  titleLink.href = link;
  titleLink.target = "_blank";
  titleLink.appendChild(content);
  return title;
};

export const buildResults = (result) => {
  const results = [];
  if (result) {
    const objectEntries = Object.entries(result);
    if (objectEntries[0].length < 2) {
      return results;
    }
    const objArray = objectEntries[0][1];
    objArray.forEach((obj) => {
      const { pageid, title, url } = obj;
      results.push({
        id: pageid,
        title: title,
        link: url
      });
    });
  }
  return results;
};

export const buildSearch = (resultsArray, append = false) => {
  const itemsContainer = document.getElementById("resultItems");
  if (!append) {
    itemsContainer.innerHTML = "";
  }
  resultsArray.map((el) => {
    const resultItem = document.createElement("div");
    const itemContent = document.createElement("div");
    console.log("link: " + el.link);
    const title = createTitle(el.title, el.link);

    // Create an iframe element to display the page contents
    const itemIframe = document.createElement("iframe");
    itemIframe.src = el.link;
    itemIframe.width = "100%";
    itemIframe.height = "300px";
    itemIframe.setAttribute("frameborder", "0");
    itemIframe.setAttribute("sandbox", "allow-same-origin allow-scripts");

    itemContent.classList.add("itemContent");
    itemContent.appendChild(itemIframe);
    resultItem.classList.add("resultItem");
    resultItem.appendChild(title);
    resultItem.appendChild(itemContent);
    itemsContainer.appendChild(resultItem);
  });
};