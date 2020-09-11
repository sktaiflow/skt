__all__ = ["numeric_eda_plot", "categorical_eda_plot"]


import logging
import pandas as pd
import numpy as np
import math
import sys

from matplotlib import pyplot as plt
import seaborn as sns

sns.set_style("darkgrid", {"font.family": ["NanumGothicCoding"]})
sns.set(rc={"figure.figsize": (10, 7)})


eda_logger = logging.getLogger("EDA_TOOLS")
eda_looger = eda_logger.setLevel(logging.INFO)

stream_hander = logging.StreamHandler(sys.stdout)
stream_hander.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_hander.setFormatter(formatter)
eda_logger.addHandler(stream_hander)


def numeric_eda_plot(
    df,
    feature_list,
    label_col,
    cols=2,
    n_samples=-1,
    plot_type="density",
    stat_yn=False,
    figsize=(7, 4),
):
    """
    Numeric feature에 대한 EDA Plot function

    Args. :
        - df           :   Pandas DataFrame 형태의 EDA대상 데이터
        - feature_list :   EDA 대상 feature list (df의 columns)
        - label_col    :   Label(or Hue) column
        - cols         :   Multi-plot 시 grid column 개수 (row 개수는 feature_list에 따라 자동으로 결정 됨)
        - n_samples    :   Label 별 sampling 할 개수 (default = -1(전수 데이터로 EDA할 경우))
        - plot_type    :   density or box (default = 'density')
        - stat_yn      :   기초 통계량 출력여부 (mean / min / max / 1q / 3q) (default : False)
        - figsize      :   (default : (7,4))

    Returns :
        matplotlib.pyplot object

    Example :
        df = load_from_hive(conn, "select * from dumbo.aips_eda_samples")
        fig = numeric_eda_plot(df, ['age','eqp_chg_cnt','real_arpu_bf_m1'], 'answer', cols = 3, n_samples = 10000,
                               plot_type='density', stat_yn=True, figsize = (7,4))
        fig
    """

    # arg type check
    assert type(feature_list) == list
    assert type(label_col) == str
    assert type(stat_yn) == bool
    assert plot_type in ["box", "density"]

    for feat in feature_list:
        assert np.issubdtype(df[feat], np.number), feat + " has %s type, should be numeric! " % (df[feat].dtype)

    ##########################################################################

    # unique lables in DF
    labels = list(df[label_col].unique())

    # label counts
    label_cnt = df.groupby(label_col).size().to_dict()

    # sampling
    if n_samples > 0:
        temp_samples_df = []
        for k, v in label_cnt.items():
            label_cnt[k] = min(n_samples, v)
            temp_samples_df.append(df[df[label_col] == k].sample(n=label_cnt[k], random_state=777))

        df = pd.concat(temp_samples_df, axis=0)

    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("LABEL COUNTS : " + str(label_cnt))
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("")

    ##########################################################################
    # set grid for multi-plot
    if len(feature_list) == 1:
        cols = 1

    rows = math.ceil(len(feature_list) / cols)
    figwidth = figsize[0] * cols
    figheight = figsize[1] * rows

    h_margin = 0.3 + 0.2 * len(labels) if stat_yn else 0.3
    fig, ax = plt.subplots(nrows=rows, ncols=cols, figsize=(figwidth, figheight))

    color_choices = ["r", "b", "goldenrod", "black", "darkorange", "g"]
    stat_names = ["AVG", "MIN", "MAX", "1Q", "3Q"]
    pattern = "%.4f"

    plt.subplots_adjust(wspace=0.3, hspace=h_margin)
    if cols != 1 or rows != 1:
        ax = ax.ravel()  # Ravel turns a matrix into a vector... easier to iterate

    ##########################################################################
    # density plot for each ax
    for i, col in enumerate(feature_list):
        temp_ax = ax[i] if cols != 1 or rows != 1 else ax
        temp_ax.clear()
        stats = []
        x_axis_text = ""

        orig_tmp_df = df[[col] + [label_col]]

        if plot_type == "density":
            # remove outliers rows (1 percentile), 99 percentile
            tmp_df = orig_tmp_df[
                (orig_tmp_df[col] >= orig_tmp_df[col].quantile(0.01))
                & (orig_tmp_df[col] <= orig_tmp_df[col].quantile(0.99))
            ]
            for j, lb in enumerate(labels):
                sns.kdeplot(
                    tmp_df[tmp_df[label_col] == lb][col],
                    shade=True,
                    color=color_choices[j],
                    label=lb,
                    ax=temp_ax,
                    gridsize=100,
                )
                # calculate simple stats
                if stat_yn:
                    tmp_stats = [
                        np.mean(orig_tmp_df[orig_tmp_df[label_col] == lb][col]),
                        np.min(orig_tmp_df[orig_tmp_df[label_col] == lb][col]),
                        np.max(orig_tmp_df[orig_tmp_df[label_col] == lb][col]),
                        np.quantile(
                            orig_tmp_df[orig_tmp_df[label_col] == lb][col],
                            0.25,
                        ),
                        np.quantile(
                            orig_tmp_df[orig_tmp_df[label_col] == lb][col],
                            0.75,
                        ),
                    ]
                    tmp_stats_text = " | ".join([pattern % i for i in tmp_stats])
                    stats.append("[{}] ".format(lb) + tmp_stats_text)

        elif plot_type == "box":
            tmp_df = orig_tmp_df
            sns.boxplot(x=label_col, y=col, data=tmp_df, ax=temp_ax)
            if stat_yn:
                for j, lb in enumerate(labels):
                    tmp_stats = [
                        np.mean(orig_tmp_df[orig_tmp_df[label_col] == lb][col]),
                        np.min(orig_tmp_df[orig_tmp_df[label_col] == lb][col]),
                        np.max(orig_tmp_df[orig_tmp_df[label_col] == lb][col]),
                        np.quantile(
                            orig_tmp_df[orig_tmp_df[label_col] == lb][col],
                            0.25,
                        ),
                        np.quantile(
                            orig_tmp_df[orig_tmp_df[label_col] == lb][col],
                            0.75,
                        ),
                    ]
                    tmp_stats_text = " | ".join([pattern % i for i in tmp_stats])
                    stats.append("[{}] ".format(lb) + tmp_stats_text)
            temp_ax.set_ylabel("")

        if stat_yn:
            stat_title = "{}".format(" | ".join(stat_names))
            x_axis_text = stat_title + "\n" + "\n".join(stats)
        temp_ax.set_xlabel(x_axis_text)

        temp_ax.set_title(col, weight="bold")
    plt.close(fig)
    return fig


def categorical_eda_plot(df, feature_list, label_col, cols=2, n_samples=-1, figsize=(7, 4)):
    """
    Categorical feature에 대한 EDA Plot function

    Args. :
        - df           :   Pandas DataFrame 형태의 EDA대상 데이터
        - feature_list :   EDA 대상 feature list (df의 columns)
        - label_col    :   Label(or Hue) column
        - cols         :   Multi-plot 시 grid column 개수 (row 개수는 feature_list에 따라 자동으로 결정 됨)
        - n_samples    :   Label 별 sampling 할 개수 (default = -1(전수 데이터로 EDA할 경우))
        - figsize      :   (default : (7,4))

    Returns :
        matplotlib.pyplot object


    Example :
        Example :
        df = load_from_hive(conn, "select * from dumbo.aips_eda_samples")
        fig = categorical_eda_plot(df, ['family_yn', 'dangol_yn','sex_cd'], 'answer', cols = 3,
                                   n_samples = 10000, figsize = (7,4))
        fig
    """

    # arg type check
    assert type(feature_list) == list
    assert type(label_col) == str
    for feat in feature_list:
        tmp_count = df[feat].nunique()
        assert tmp_count < 100, "you have %d unique values, it is not categorical value" % (tmp_count)

    ##########################################################################

    # label counts
    label_cnt = df.groupby(label_col).size().to_dict()

    # sampling
    if n_samples > 0:
        temp_samples_df = []
        for k, v in label_cnt.items():
            label_cnt[k] = min(n_samples, v)
            temp_samples_df.append(df[df[label_col] == k].sample(n=label_cnt[k], random_state=777))

        df = pd.concat(temp_samples_df, axis=0)

    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("LABEL COUNTS : " + str(label_cnt))
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("")

    ##########################################################################
    # set grid for multi-plot
    if len(feature_list) == 1:
        cols = 1

    rows = math.ceil(len(feature_list) / cols)
    figwidth = figsize[0] * cols
    figheight = figsize[1] * rows

    h_margin = 0.3
    fig, ax = plt.subplots(nrows=rows, ncols=cols, figsize=(figwidth, figheight))

    plt.subplots_adjust(wspace=0.3, hspace=h_margin)
    if cols != 1 or rows != 1:
        ax = ax.ravel()  # Ravel turns a matrix into a vector... easier to iterate

    ##########################################################################
    # bar plot for each ax
    for i, col in enumerate(feature_list):
        temp_ax = ax[i] if cols != 1 or rows != 1 else ax
        temp_ax.clear()
        tmp_df = df.loc[:, [col] + [label_col]]

        tmp_df = tmp_df.groupby([label_col] + [col]).size().to_frame("perc")
        tmp_df = (
            tmp_df.groupby(level=0)
            .apply(lambda x: 100 * x / float(x.sum()))
            .groupby(level=[0])
            .cumsum()
            .reset_index()
            .sort_values(["perc"], ascending=False)
        )

        sns.barplot(
            x=label_col,
            y="perc",
            hue=col,
            data=tmp_df,
            dodge=False,
            ax=temp_ax,
        )
        temp_ax.legend(title="")

        temp_ax.title.set_text(col)

    plt.close(fig)
    return fig
